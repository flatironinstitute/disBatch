import argparse, cmd, glob, json, logging, re, subprocess, sys, time
import os, os.path
from collections import defaultdict
from multiprocessing import Process

import disBatch1
from disBatch1 import DisBatchServer, DisBatchInfo
from disBatch1 import batchArgParser, contextArgs, getUniqueID, extendBatchArgs

try:
    import kvsstcp
except ImportError:
    print('''
Could not find disBatch components in:

  %s

Try setting the enviornment variable "DISBATCH_ROOT" to the directory
containing the script "disBatch.py", which should have a subdirectory
named "kvsstcp".
'''%ImportDir, file=sys.stderr)
    sys.exit(1)

db_kvs   = re.compile(r'DISBATCH_KVSSTCP_HOST=(\S+)', re.I)

def getKVSServerFromFile (kvsinfo_dir='.', kvsinfo_file=None):
    if not kvsinfo_file:
        kvsinfo = glob.glob('{}/*_kvsinfo.txt'.format(kvsinfo_dir))
        if not kvsinfo:
            print("Can not find kvsinfo file in directory {}.".format(kvsinfo_dir))
            return None
        kvsinfo.sort(key=os.path.getmtime)
        kvsinfo_file  = kvsinfo[-1]        # if multiple, get the latest one

    print("kvsinfo_file={}".format(kvsinfo_file))
    with open (kvsinfo_file, 'r') as f:
         kvsserver = f.read().strip()
    return kvsserver

def printDBInfoHeader ():
    print("Unique Name".ljust(36), "Name".ljust(12), "Unique Id".ljust(48), "Work Dir")
    print("-"*100)

def getUniqueName (uniqueId):
    return os.path.split(uniqueId)[-1]

class DisbatchShell(cmd.Cmd):
    intro   = 'Welcome to the disBatch shell.   Type help or ? to list commands.\n'
    prompt  = '> '
    file    = None
    servers = {}
    curr_server = None
    curr_kvs    = None
    argp    = batchArgParser()

    def __init__ (self):
        cmd.Cmd.__init__(self)
        self.argp              = batchArgParser ()
        contextArgs (self.argp)


    #TODO: must be ./tmp/test1 or ./
    def getNameFromPath (self, path):
        return path.split('/')[-1]
    def getPathFromName (self, name):
        return './tmp/{}'.format(name)
    def getDbUtil (self, path):
        dbUtils       = glob.glob('{}/*_dbUtil.sh'.format(path))
        if not dbUtils:
            print("Can not fine dbUtil.sh under {}".format(path))
            return
        else:
            dbUtil        = dbUtils[-1]
            print ("latest dbUtil {}".format(dbUtil))
            return dbUtil
    def getKVS (self, dbUtil):
        with open (dbUtil, 'r') as f:
            s = f.read()
            m = db_kvs.search(s)
            return m.group(1)

    def printDbInfo (self, uName, dbInfo):
        if self.curr_server and self.curr_server.uniqueId == dbInfo.uniqueId:
            print("*{:35} {:12} {:48} {}".format(uName, dbInfo.name, dbInfo.uniqueId, dbInfo.wd))
        else:        
            print("{:36} {:12} {:48} {}".format(uName, dbInfo.name, dbInfo.uniqueId, dbInfo.wd))


 
    def init_servers (self):
        for x in os.popen('pgrep -a -f "disBatch(1.py|) -S"'):
            #844105 /usr/bin/python3 /cm/shared/sw/pkg/flatiron/disBatch/2.0-beta/disBatch -S -p ./tmp/test11 4KTasksRep
            args = x.split()[3:]
            args = self.argp.parse_args(args)

            args.kvsserver = getKVSServerFromFile (args.prefix)
            kvs            = kvsstcp.KVSClient(args.kvsserver)
            dbInfo         = kvs.view('.db info')
            uid            = getUniqueName(dbInfo.uniqueId)
            #print(args)
            #print(dbInfo.args)

            self.servers[uid] = (dbInfo, kvs)

        print ('servers={}'.format(self.servers))

    def update_servers (self):
        dead_db = []
        for uid, (dbInfo, kvsc) in self.servers.items():
            try:
                j = kvsc.view('DisBatch status')
            except:
                uid = getUniqueName(dbInfo.uniqueId)
                print("DisBatch {} is finished".format(uid))
                dead_db.append(uid)

        for uid in dead_db:
            self.servers.pop(uid)

        if self.curr_server and getUniqueName(self.curr_server.uniqueId) in dead_db:  # curr_server is finished
            self.curr_server = None

    def server_subprocess (self, arg):
        args          = ['-S', '-p'] + arg.split()

        #path          = self.getPathFromName (name)
        #if not os.path.exists (path):
        #    os.mkdir(path)
        #elif not os.path.isdir(path):
        #    print("ERROR: {} needs to be a directory.".format(path))
        #    return
        #if not os.path.isfile(taskfile):
        #    print("ERROR: {} needs to be a task file.".format(taskfile))
        #    return

        proc           = subprocess.Popen(["disBatch"] + args, stdout=subprocess.PIPE)
        time.sleep(1)

        args           = self.argp.parse_args(args)
        extendBatchArgs (args)
        #print (args)

        args.kvsserver = getKVSServerFromFile (args.prefix)
        #print("kvsserver={}".format(args.kvsserver))

        kvs    = kvsstcp.KVSClient(args.kvsserver)
        dbInfo = kvs.view('.db info')
        print("dbInfo.args={}".format(dbInfo.args))

        return dbInfo, kvs

    def check_curr_server(self):
        self.update_servers ()
        if not self.curr_server:
            print("Please choose a disBatch server to which add context. You can use either add_server or use_server.")
            return False
        return True

    def stop_context (self, kvs, cRank):
        #cRank = engines[target]['cRank']
        #r = popYNC('Stopping context {cLabel:s} ({cRank:d})'.format(**engines[target]), S, inq)
        #if r == 'Y':
        try:
            #msg = 'Asking controller to stop context %r'%cRank
            print("Asking controller to shutdown context {}...".format(cRank))
            kvs.put('.controller', ('stop context', cRank))
            #for rank, e in engines.items():
            #    if e['cRank'] == cRank:
            #        localEngineStatus[rank] = 'requesting shutdown'
        except socket.error:
            pass


    def do_show_servers(self, arg):
        'Show the existing disBatch stand-alone servers'
        self.update_servers()     # call this to avoid show finished server

        printDBInfoHeader ()
        for uid, (dbInfo, kvs) in self.servers.items():
            self.printDbInfo (uid, dbInfo)

    def do_add_server(self, arg):
        'Add disBatch stand-alone server with a name and a task file.'
        #add_server /tmp/dbTestXXXX ./4KTasksRep
        #disBatch -S -p ./tmp/dbTestXXXX ./4KTasksRep
        #args          = arg.split()
        #args          = ["-S", "-p"] + args
        #print(args)

        #args          = self.argp.parse_args(args)
        #extendBatchArgs (args)
        #print(args)
        #if args.uniqueId in self.servers:
        #    print("ERROR: A server with uniqueId {} already exists".format(name))
        #    return

        #server        = DisBatchServer (args)
        #server.startDriver_bg ()
        #self.servers[server.dbInfo.uniqueId] = server.dbInfo

        dbInfo, kvs       = self.server_subprocess (arg)
        uid               = getUniqueName(dbInfo.uniqueId)
        self.servers[uid] = (dbInfo, kvs)
        self.do_use_server (uid)

        print("Add new disBatch server and set it to the current server")
        printDBInfoHeader ()
        self.printDbInfo(uid, dbInfo)

    def do_use_server (self, uid):
        'Use disBatch server with UniqueId for the following operations'
        self.curr_server, self.curr_kvs = self.servers[uid]

        self.update_servers ()
        self.printDbInfo(uid, self.curr_server)

    def get_disb_status (self, kvsc):
        try:
            j = kvsc.view('DisBatch status')
        except:
            print("ERROR getting DisBatch status from {}".format(kvsserver))
            return None

        if j != b'<Starting...>':
            statusd = json.loads(j)

            now = time.time()

            # convert keys back to ints after json transform.
            engines  = {int(k): v for k, v in statusd['engines'].items()}
            contexts = {int(k): v for k, v in statusd['contexts'].items()}

            ee       = engines.values()
            statusd['slots']    = sum([len(e['cylinders']) for e in ee if e['status'] == 'running'])
            statusd['finished'] = sum([e['finished'] for e in ee])
            statusd['failed']   = sum([e['failed'] for e in ee])
            #tuin     = uniqueIdName if len(uniqueIdName) <= 40 else (uniqueIdName[:17] + '...' + uniqueIdName[-20:])
            #label    = f'Run label: {tuin:<40s}           Status: {statusd["more"]:15s}'
            #header.append((['Slots {slots:5d}                  Tasks: Finished {finished:7d}      Failed{failed:5d}      Barrier{barriers:3d}'.format(**statusd)], CPCB))
            #                       '01234 012345678901 01234567890123456789 0123456  0123456 0123456789 0123456789 0123456'
            #header.append(([Vertical] + ['Rank    Context           Host          Last     Avail   Assigned   Finished   Failed'] + [Vertical], CPCB))
            #assert len(header) == HeaderLength

            #ee = sorted(engines.items())
            #content = []
            for cRank, c in contexts.items():
                c['engines'] = []
            for rank, engine in engines.items():
                if engine['status'] == 'stopped': continue            #not displaying stopped engine
                engine['slots'] = len(engine['cylinders'])
                engine['delay'] = now - engine['last']
                cRank           = engine['cRank']
                contexts[cRank]['engines'].append(rank)
            #    engine['cLabel'] = contexts[engine['cRank']]['label']
            #    content.append((rank, '{rank:5d} {cLabel:12.12s} {hostname:20.20s} {delay:6.0f}s {slots:7d} {assigned:10d} {finished:10d} {failed:7d}'.format(**engine)))

            return contexts, engines
        
        print("DisBatch stuatus: <Starting ...> \nPlease come back in a few seconds to check.")
        return None, None

    def printContext (cRank, context):
        if context:
            dbInfo = context['dbInfo']
            print ("{:12.12s} {:20}".format(context['sysid'], dbInfo['name']))

    def printEngine (eRank, engine):
        if engine:
            print ('\t{rank:5d} {hostname:20.20s} {delay:6.0f}s {slots:7d} {assigned:10d} {finished:10d} {failed:7d}'.format(**engine))

    def printContextEngine (contexts, engines):
        for cRank, ctx in contexts.items():
            DisbatchShell.printContext(cRank, ctx)
            for eRank in ctx['engines']:
                DisbatchShell.printEngine (eRank, engines[eRank])

    def do_show_contexts (self, arg):
        'Show the contexts of current disBatch server'
        if not self.check_curr_server():
            return
        #print(self.curr_server)

        uniqueIdName = os.path.split(self.curr_server.uniqueId)[-1]
        kvsc         = self.curr_kvs

        contexts, engines = self.get_disb_status (self.curr_kvs)
        #print("engines={}".format (engines))
        print("contexts={}".format(contexts))

        if contexts:
            DisbatchShell.printContextEngine (contexts, engines)

    def do_add_context(self, context_arg):
        'Add an execution context to the current disBatch server'
        #sbatch -N 2 -p scc /mnt/home/yliu/projects/slurm/disBatch/tmp/dbTestXXXX/4KTasksRep_disBatch_210923173212_590_dbUtil.sh
        dbInfo     = self.curr_server
        if not self.check_curr_server():
            print("Please choose a disBatch server to be the current server using: use_server disBatch_UniqueId")
            return
        DbUtilPath = dbInfo.uniqueId + "_dbUtil.sh"
        proc       = subprocess.Popen(context_arg.split() + [DbUtilPath], stdout=subprocess.PIPE)
        print("Please check the context after a few seconds using: show_contexts")
        #print(context_arg.split() + [DbUtilPath])

    def do_shutdown_context (self, cRank):
        'Remove a context and all the engines of it'
        self.stop_context (self.curr_kvs, cRank)
        print("shutown context {}".format(cRank))

    def shutdown_engine1 (self, kvs, eRank):
        try:
            print("Asking controller to stop engine {}".format(eRank))
            kvs.put('.controller', ('stop engine', eRank))
        except socket.error:
            print("Have a socket error.")


    def do_shutdown_engine (self, arg):
        "Remove a context's engine"
        eRank = int(arg)
        self.shutdown_engine1 (self.curr_kvs, eRank)
        print("shutown engine")

    def do_quit(self, arg):
        'Leave everything running as it is, and exit:  BYE'
        print('Thank you for using disBatch')
        self.close()
        return True

    def preloop (self):
        disBatch1.logger = logging.getLogger('DisBatch Shell')
        lconf  = {'format': '%(asctime)s %(levelname)-8s %(name)-15s: %(message)s', 'level': logging.INFO}
        lconf['filename'] = 'DisBatch Shell.log'
        logging.basicConfig(**lconf)
        self.init_servers ()

    def precmd(self, line):
        if not line:
            return line
        args    = line.split()
        args[0] = args[0].lower()
        line    = ' '.join(args)
        if self.file and 'playback' not in line:
            print(line, file=self.file)
        return line

    def close(self):
        if self.file:
            self.file.close()
            self.file = None
        #close created process?

    def emptyline(self):
        pass

def parse(arg):
    'Convert a series of zero or more numbers to an argument tuple'
    return tuple(map(int, arg.split()))

if __name__ == '__main__':
    DisbatchShell().cmdloop()
