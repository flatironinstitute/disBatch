#!/usr/bin/python
from __future__ import print_function
import json, logging, os, random, re, signal, socket, subprocess as SUB, sys, time

from ast import literal_eval
from collections import defaultdict as DD

try:
    from queue import Queue, Empty
except ImportError:
    from Queue import Queue, Empty
from threading import enumerate as ListThreads, Thread

DisBatchPath, ImportDir, PathsFixed = None, None, False # <= May need to set these, setting PathsFixed to True as well.

# Handle self-modification invocation early, before messing with paths and other imports
if '__main__' == __name__ and sys.argv[1:] == ["--fix-paths"]:
    import tempfile
    DisBatchPath = os.path.realpath(__file__)
    if not os.path.exists(DisBatchPath):
        print('Unable to find myself; set DisBatchPath and ImportDir manually at the top of disBatch.py.', file=sys.stderr)
        sys.exit(1)
    DisBatchDir = os.path.dirname(DisBatchPath)
    with open(DisBatchPath, 'r') as fi:
        with tempfile.NamedTemporaryFile('w', prefix='disBatch.py.', dir=DisBatchDir, delete=False) as fo:
            found = False
            for l in fi:
                if l.startswith('DisBatchPath, ImportDir, PathsFixed ='):
                    assert not found
                    found = True
                    l = 'DisBatchPath, ImportDir, PathsFixed = %r, %r, True\n'%(DisBatchPath, DisBatchDir)
                    print("Changing path info to %r"%l, file=sys.stderr)
                fo.write(l)
            assert found
            os.fchmod(fo.fileno(), os.fstat(fi.fileno()).st_mode)
    os.rename(DisBatchPath, DisBatchPath+'.prev')
    os.rename(fo.name, DisBatchPath)
    sys.exit(0)

if not PathsFixed:
    # Try to guess
    DisBatchPath = os.path.realpath(__file__)
    ImportDir = os.path.dirname(DisBatchPath)

PythonPath = os.getenv('PYTHONPATH', '')
if ImportDir:
    # to find kvsstcp:
    sys.path.append(ImportDir)
    # for subprocesses:
    PythonPath = PythonPath + ':' + ImportDir if PythonPath else ImportDir
    os.environ['PYTHONPATH'] = PythonPath

try:
    import kvsstcp
except ImportError:
    if PathsFixed:
        print('This script is looking in the wrong place for "kvssctp". Try running "%s --fix-paths" or editing it by hand.'%DisBatchPath, file=sys.stderr)
    else:
        print('''
Could not find kvsstcp. If there is a "kvsstcp" directory in "%s",
try running "%s --fix-paths". Otherwise review the installation
instructions.
'''%(ImportDir, DisBatchPath), file=sys.stderr)
    sys.exit(1)
    
myHostname = socket.gethostname()
myPid = os.getpid()

# Note that even though these are case insensitive, only lines that start with upper-case '#DISBATCH' prefixes are tested
dbbarrier   = re.compile('^#DISBATCH BARRIER(?: (.+)?)?$', re.I)
dbcomment   = re.compile('^\s*(#|$)')
dbprefix    = re.compile('^#DISBATCH PREFIX (.*)$', re.I)
dbrepeat    = re.compile('^#DISBATCH REPEAT\s+(?P<repeat>[0-9]+)(?:\s+start\s+(?P<start>[0-9]+))?(?:\s+step\s+(?P<step>[0-9]+))?(?: (?P<command>.+))?\s*$', re.I)
dbsuffix    = re.compile('^#DISBATCH SUFFIX (.*)$', re.I)
dbperengine = re.compile('^#DISBATCH (?:PERENGINE|PERNODE) (.*)$', re.I) # PERNODE is deprecated. TODO: warn about this?

def compHostnames(h0, h1):
    return h0.split('.', 1)[0] == h1.split('.', 1)[0]

def waitTimeout(sub, timeout, interval=1):
    r = sub.poll()
    while r is None and timeout > 0:
        time.sleep(interval)
        timeout -= interval
        r = sub.poll()
    return r

def killPatiently(sub, name, timeout=15):
    if not sub: return
    r = sub.poll()
    if r is None:
        logger.info('Waiting for %s to finish...', name)
        r = waitTimeout(sub, timeout)
    if r is None:
        logger.warn('Terminating %s...', name)
        try:
            sub.terminate()
        except OSError:
            pass
        r = waitTimeout(sub, timeout)
    if r is None:
        logger.warn('Killing %s.', name)
        try:
            sub.kill()
        except OSError:
            pass
        r = sub.wait()
    if r:
        logger.info("%s returned %d", name, r)
    return r

def register(kvs, which):
    key = '%d'%(10e7*random.random())
    kvs.put('.controller', ('register', (which, key)))
    return kvs.get(key)

class DisBatchInfo(object):
    def __init__(self, args, name, uniqueId, wd):
        self.args, self.name, self.uniqueId, self.wd = args, name, uniqueId, wd
        
class BatchContext(object):
    def __init__(self, sysid, dbInfo, rank, nodes, cylinders, args, contextLabel=None):
        if contextLabel is None:
            contextLabel = 'context%05'%rank
        self.sysid, self.dbInfo, self.rank, self.nodes, self.cylinders, self.args, self.label = sysid, dbInfo, rank, nodes, cylinders, args, contextLabel
            
        self.error = False # engine errors (non-zero return values)
        self.kvsKey = '.context %d'%rank
        self.retireCmd = None

    def __str__(self):
        return 'Context type: %s\nLabel: %s\nNodes: %r\nCylinders: %r\n'%(self.sysid, self.label, self.nodes, self.cylinders)

    def launch(self, kvs):
        '''Launch the engine processes on all the nodes by calling launchNode for each.'''
        kvs.put(self.kvsKey, self)
        kvs.put('.controller', ('context info', self))
        self.engines = dict() # live subprocesses
        for n in self.nodes:
            self.engines[n] = self.launchNode(n)

    def poll(self):
        '''Check if any engines have stopped.'''
        for n, e in list(self.engines.items()):
            r = e.poll()
            if r is not None:
                logger.info('Engine %s exited: %d', n, r)
                del self.engines[n]
                self.retireNode(n, r)

    def launchNode(self, node):
        '''Launch an engine for a single node.  Should return a subprocess handle (unless launch itself is overridden).'''
        raise NotImplementedError('%s.launchNode is not implemented' % type(self))

    def retireEnv(self, node, ret):
        '''Generate an environment for running the retirement command for a given node.'''
        env = os.environ.copy()
        env['NODE'] = node
        env['RETCODE'] = str(ret)
        env['ACTIVE'] = ','.join(self.engines.keys())
        env['RETIRED'] = ','.join(set(self.nodes).difference(self.engines))
        return env

    def retireNode(self, node, ret):
        '''Called when a node has exited.  May be overridden to release resources.'''
        if ret: self.error = True
        if self.retireCmd:
            logger.info('Retiring node "%s" with command %s', node, str(self.retireCmd))
            env = self.retireEnv(node, ret)
            try:
                SUB.check_call(self.retireCmd, close_fds=True, shell=True, env=env)
            except Exception as e:
                logger.warn('Retirement planning needs improvement: %s', repr(e))
        else:
            logger.info('Retiring node "%s" (no-op)', node)

    def finish(self):
        '''Check that all engines completed successfully and return True on success.'''
        for n, e in self.engines.items():
            r = killPatiently(e, 'engine ' + n)
            if r: self.error = True # also handled by retireNode
        return not self.error

    def setNode(self, node=None):
        '''Try to determine the hostname of this engine from the pov of the launcher.'''
        # This is just a fallback. Implementations should try to determine node as appropriate.
        # Could just default to node=myHostname, but then we lose special domain-name matching
        if not node:
            for n in self.nodes:
                if compHostnames(n, myHostname):
                    node = n
                    break
        self.node = node
        try:
            self.nodeId = self.nodes.index(self.node)
        except ValueError:
            # Should we instead assume 0 or carry on with none?
            raise LookupError('Couldn\'t find nodeId for %s in "%s".' % (node or myHostname, self.nodes))

# Convert nodelist format (slurm specific?) to an expanded list of nodes.
#    nl     => hosts[,nl]
#    hosts  => prefix[\[ranges\]]
#    ranges => range[,ranges]
#    range  => lo[-hi]
# where lo and hi are numbers
def nl2flat(nl):
    return SUB.check_output(["scontrol", "show", "hostnames", nl], universal_newlines=True).splitlines()

class SlurmContext(BatchContext):
    def __init__(self, dbInfo, rank, args):
        jobid = os.environ['SLURM_JOBID']
        nodes = nl2flat(os.environ['SLURM_NODELIST'])

        cylinders = []
        for tr in os.environ['SLURM_TASKS_PER_NODE'].split(','):
            m = re.match(r'([^\(]+)(?:\(x([^\)]+)\))?', tr)
            c, m = m.groups()
            if m == None: m = '1'
            cylinders += [int(c)]*int(m)

        contextLabel = args.label if args.label else 'J%d'%jobid
        super(SlurmContext, self).__init__('SLURM', dbInfo, rank, nodes, cylinders, args, contextLabel)
        self.driverNode = None
        self.retireCmd = "scontrol update JobId=\"$SLURM_JOBID\" NodeList=\"${DRIVER_NODE:+$DRIVER_NODE,}$ACTIVE\""

    def launchNode(self, n):
        lfp = '%s_%s_%s_engine_wrap.log'%(self.dbInfo.uniqueId, self.label, n)
        return SUB.Popen(['srun', '-N', '1', '-n', '1', '-w', n, DisBatchPath, '--engine', '-n', n, kvsserver, self.kvsKey], stdout=open(lfp, 'w'), stderr=SUB.STDOUT, close_fds=True)

    def retireEnv(self, node, ret):
        env = super(SlurmContext, self).retireEnv(node, ret)
        if self.driverNode:
            env['DRIVER_NODE'] = self.driverNode
        return env

    def retireNode(self, node, ret):
        if compHostnames(node, myHostname):
            self.driverNode = node
        super(SlurmContext, self).retireNode(node, ret)

    def setNode(self, node=None):
        super(SlurmContext, self).setNode(node or os.getenv('SLURMD_NODENAME'))

#TODO:
#class GEContext(BatchContext):
#class LSFContext(BatchContext):
#class PBSContext(BatchContext):

# The ssh context should be generally applicable when all else fails
# (or there is no resource manager).
#
# To use, set the environment variable DISBATCH_SSH_NODELIST. E.g.:
#     DISBATCH_SSH_NODELIST=hostname0:4,hostname1:5
# indicates 4 cylinders (execution entities) should run on hostname0
# and 5 on hostname1.
#
# You can also specify a "job id" via DISBATCH_SSH_JOBID. If you do
# not provide one, one will be created from the PID and epoch time.
class SSHContext(BatchContext):
    def __init__(self, dbInfo, rank, args):
        nodelist = args.ssh_node if args.ssh_node else os.getenv('DISBATCH_SSH_NODELIST')
        contextLabel = args.label if args.label else 'SSH%d'%rank

        cylinders, nodes = [], []
        if type(nodelist) is not str: nodelist = ','.join(nodelist)
        for p in nodelist.split(','):
            p = p.strip()
            if not p: continue
            try:
                n, e = p.rsplit(':', 1)
                e = int(e)
            except ValueError:
                raise ValueError('SSH nodelist items must be HOST:COUNT')
            if n == 'localhost': n = myHostname
            nodes.append(n)
            cylinders.append(e)

        super(SSHContext, self).__init__('SSH', dbInfo, rank, nodes, cylinders, args, contextLabel)

    def launchNode(self, n):
        prefix = [] if compHostnames(n, myHostname) else ['ssh', n, 'PYTHONPATH=' + PythonPath]
        lfp = '%s_%s_%s_engine_wrap.log'%(self.dbInfo.uniqueId, self.label, n)
        return SUB.Popen(prefix + [DisBatchPath, '--engine', '-n', n, kvsserver, self.kvsKey], stdin=open(os.devnull, 'r'), stdout=open(lfp, 'w'), stderr=SUB.STDOUT, close_fds=True)


def probeContext(dbInfo, rank, args):
    if 'SLURM_JOBID' in os.environ: return SlurmContext(dbInfo, rank, args)
    #if ...: return GEContext()
    #if ...: LSFContext()
    #if ...: PBSContext()
    if 'DISBATCH_SSH_NODELIST' in os.environ: return SSHContext(dbInfo, rank, args)

class TaskInfo(object):
    kinds = {'B': 'barrier', 'C': 'check barrier', 'D': 'done', 'N': 'normal', 'P': 'per node', 'S': 'skip'}

    def __init__(self, taskId, taskStreamIndex, taskRepIndex, taskAge, taskCmd, taskKey, kind = 'N', bKey=None):
        self.taskId, self.taskStreamIndex, self.taskRepIndex, self.taskAge, self.taskCmd, self.taskKey, self.bKey = taskId, taskStreamIndex, taskRepIndex, taskAge, taskCmd, taskKey, bKey
        assert kind in TaskInfo.kinds
        self.kind = kind

    def __str__(self):
        # If this changes, update parseStatusFile below and disBatcher.py too
        return '\t'.join([str(x) for x in [self.taskId, self.taskStreamIndex, self.taskRepIndex, self.taskAge, self.kind, repr(self.taskCmd)]])

class TaskReport(object):
    def __init__(self, taskInfo, host = myHostname, pid = myPid, returncode = 0, start = 0, end = 0, outbytes = 0, outdata = '', errbytes = 0, errdata = ''):
        self.taskInfo = taskInfo
        self.host, self.pid, self.returncode, self.start, self.end, self.outbytes, self.outdata, self.errbytes, self.errdata = host, pid, returncode, start, end, outbytes, outdata, errbytes, errdata
        self.engineReport = None # This will be filled in by EngineBlock.

    def flags(self):
        if self.taskInfo.kind in 'NP':
            return (  ('R' if self.returncode  else ' ')
                    + ('O' if self.outbytes    else ' ')
                    + ('E' if self.errbytes    else ' ')
                    + ('P' if self.taskInfo.kind == 'P' else  ''))
        else:
            return self.taskInfo.kind + '    '
        
    def __str__(self):
        # If this changes, update parseStatusFile below and disBatcher.py too
        ti = self.taskInfo
        return '\t'.join([str(x) for x in [self.flags(), ti.taskId, ti.taskStreamIndex, ti.taskRepIndex, ti.taskAge, self.host, self.pid, self.returncode, self.end - self.start, self.start, self.end, self.outbytes, repr(self.outdata), self.errbytes, repr(self.errdata), repr(ti.taskCmd)]])

    def __eq__(self, other):
        if type(self) is not type(other): return False
        sti, oti = self.taskInfo, other.taskInfo
        return sti.taskId == oti.taskId and sti.taskStreamIndex == oti.taskStreamIndex and sti.taskRepIndex == oti.taskRepIndex and sti.taskAge == oti.taskAge and sti.taskCmd == oti.taskCmd # and sti.taskKey == oti.taskKey

    def __ne__(self, other):
        return not self == other

def parseStatusFiles(*files):
    status = dict()
    for f in files:
        with open(f, 'r') as s:
            for l in s:
                d = l.split('\t')
                if len(d) != 15:
                    logger.warn('Invalid status line (ignoring): %r'%l)
                    continue
                if d[0] in 'BCDP': continue
                tr = TaskReport(TaskInfo(int(d[1]), int(d[2]), int(d[3]), int(d[4]), literal_eval(d[15]), '.task', kind='S'), d[5], int(d[6]), int(d[7]), float(d[9]), float(d[10]), int(d[11]), literal_eval(d[12]), int(d[13]), literal_eval(d[14]))
                try:
                    # successful tasks take precedence
                    if status[tr.taskInfo.taskId].returncode <= tr.taskinfo.returncode: continue
                except KeyError:
                    pass
                status[tr.taskInfo.taskId] = tr
    return status

##################################################################### DRIVER

# When the user specifies tasks will be passed through a KVS, this
# class generates an interable that feeds task from the KVS.
class KVSTaskSource(object):
    def __init__(self, kvs):
        self.kvs = kvs.clone()
        self.name = self.kvs.get('task source name', False)
        self.taskkey = self.name + ' task'
        self.resultkey = self.name + ' result %d'
        self.donetask = self.name + ' done!'

    def __next__(self):
        t = self.kvs.get(self.taskkey, False)
        if t == self.donetask:
            self.kvs.close()
            raise StopIteration
        return t

    def done(self):
        kvs = self.kvs.clone()
        kvs.put(self.taskkey, self.donetask)
        kvs.close()

# When the user specifies a command that will be generating tasks,
# this class wraps the command's execution so we can trigger a
# shutdown if the user's command fails to send an indication that task
# generation is done.
class TaskProcess():
    def __init__(self, taskSource, command, **kwargs):
        self.taskSource = taskSource
        self.command = command
        self.p = SUB.Popen(command, **kwargs)
        self.r = None

    def poll(self):
        if self.r is not None: return
        self.r = self.p.poll()
        if self.r is None: return
        # TODO: send done on success
        logger.info('Task generating command has exited: %s %d.', repr(self.command), self.r)
        # post a done just in case the process didn't
        self.taskSource.done()

# Given a task source (generating task command lines), parse the lines and
# produce a TaskInfo generator.
def taskGenerator(tasks):
    age = 0 # Number of per node commands that preceded the task.
    tsx = 0 # "line number" of current task
    taskCounter = 0 # next taskId
    prefix = suffix = ''

    while 1:
        tsx += 1
        try:
            t = next(tasks)
        except StopIteration:
            # Signals there will be no more tasks.
            break

        # Split on newlines.
        #
        # This allows tasks submitted through kvs with or without newlines,
        # including multiple tasks per item, or from files (always with single
        # trailing newline).
        #
        # Note that multiple lines in the same item get the same streamIndex,
        # but this shouldn't be a problem.  (Alternatively could increment tsx
        # inside this loop instead.)
        for t in t.splitlines():

            logger.debug('Task: %s', t)

            if t.startswith('#DISBATCH '):
                m = dbprefix.match(t)
                if m:
                    prefix = m.group(1)
                    continue
                m = dbsuffix.match(t)
                if m:
                    suffix = m.group(1)
                    continue
                m = dbrepeat.match(t)
                if m:
                    repeats, rx, step = int(m.group('repeat')), 0, 1
                    g = m.group('start')
                    if g: rx = int(g)
                    g = m.group('step')
                    if g: step = int(g)
                    logger.info('Processing repeat: %d %d %d', repeats, rx, step)
                    cmd = prefix + (m.group('command') or '') + suffix
                    while repeats > 0:
                        yield TaskInfo(taskCounter, tsx, rx, age, cmd, '.task')
                        taskCounter += 1
                        rx += step
                        repeats -= 1
                    continue
                mpe, mb = dbperengine.match(t), dbbarrier.match(t)
                if mpe or mb:
                    bKey = None
                    if mpe:
                        cmd = prefix + mpe.group(1) + suffix
                        kind = 'P'
                    else:
                        cmd = t
                        bKey = mb.group(1)
                        if bKey == 'CHECK':
                            kind = 'C'
                            bKey = None
                        else:
                            kind = 'B'

                    yield TaskInfo(taskCounter, tsx, -1, age, cmd, '.per engine %d'%age, kind=kind, bKey=bKey)
                    age += 1
                    taskCounter += 1
                    continue
                logger.error('Unknown #DISBATCH directive: %s', t)

            if dbcomment.match(t):
                # Comment or empty line, ignore
                continue

            yield TaskInfo(taskCounter, tsx, -1, age, prefix + t + suffix, '.task')
            taskCounter += 1

    logger.info('Processed %d tasks.', taskCounter)

def statusTaskFilter(tasks, status, retry=False, force=False):
    while True:
        t = next(tasks)
        s = status.get(t.taskId)
        if s and (not retry or s.returncode == 0):
            # skip
            if s != t:
                msg = 'Recovery status file task mismatch %s:\n' + str(s) + '\n' + str(t)
                if force:
                    logger.warn(msg, '-- proceeding anyway')
                else:
                    raise Exception(msg % '(use --force-resume to proceed anyway)')
            yield s
        else:
            yield t

# Main control loop that sends new tasks to the execution engines.
class Feeder(Thread):
    def __init__(self, kvs, ageQ, tasks, slots):
        super(Feeder, self).__init__(name='Feeder')
        self.kvs = kvs.clone()
        self.ageQ = ageQ
        self.age = 0
        self.taskGenerator = tasks
        self.slots = slots
        # just used for status reporting (not thread safe):
        self.shutdown = None

        self.daemon = True
        self.start()

    def run(self):
        try:
            self.main()
        except Exception as e:
            logger.exception('Feeder')
            estr = 'Feeder exception: ' + str(e)
            self.kvs.put('.controller', ('feeder exception', None))
            raise

    def main(self):
        lastId = -1
        while True:
            if self.shutdown is not None:
                break
            try:
                tinfo = next(self.taskGenerator)
            except StopIteration:
                self.kvs.put('.controller', ('no more tasks', lastId+1))
                self.shutdown = 'No more tasks.'
                break
            
            lastId = tinfo.taskId
            if tinfo.kind in 'BCDS':
                self.kvs.put('.controller', ('special task', tinfo))
                continue

            # At this point, we have a task that needs to go to the engines.
            if tinfo.kind == 'N':
                while tinfo.taskAge != self.age:
                    # Don't get ahead of ourselves.
                    self.age = self.ageQ.get()
                    logger.info('Feeder stepping age to %d, looking to reach %d.', self.age, tinfo.taskAge)
                # Do a little flow control for normal tasks. Wait for a slot.
                self.slots.get()

            logger.info('Posting task: %s', tinfo)
            self.kvs.put('.controller', ('task', tinfo))

        self.kvs.close()

# Main control loop that processes completed tasks.
class Driver(Thread):
    def __init__(self, kvs, db_info, tasks, trackResults=None, mailTo=None, mailFreq=1):
        super(Driver, self).__init__(name='Driver')
        self.kvs = kvs.clone()
        self.db_info = db_info
        self.kvs.put('.common env', {'DISBATCH_JOBID': str(self.db_info.uniqueId), 'DISBATCH_NAMETASKS': self.db_info.name})
        self.trackResults = trackResults
        self.mailTo = mailTo
        self.mailFreq = mailFreq

        self.age = 0
        self.ageQ = Queue()
        self.ageQ.put(0)
        self.slots = Queue()
        self.feeder = Feeder(self.kvs, self.ageQ, tasks, self.slots)

        self.barriers = []
        self.contextCount = 0
        self.contexts = {}
        self.currentReturnCode = 0
        self.engineCount = 0
        self.engines = {}
        self.failed = 0
        self.finished = 0
        self.statusFile = open(db_info.uniqueId + '_status.txt', 'w+')
        self.statusLastOffset = self.statusFile.tell()

        self.daemon = True
        self.start()

    def sendNotification(self):
        try:
            import smtplib
            from email.mime.text import MIMEText
            self.statusFile.seek(self.statusLastOffset)
            mailTo = self.db_info.args.mailTo
            msg = MIMEText('Last %d:\n\n'%self.db_info.args.mailFreq + self.statusFile.read())
            msg['Subject'] = '%s has completed %d tasks'%(self.db_info.uniqueId, self.finished)
            if self.failed:
                msg['Subject'] += ' (%d failed)'%self.failed
            msg['From'] = mailTo
            msg['To'] = mailTo
            s = smtplib.SMTP()
            s.connect()
            s.sendmail([mailTo], [mailTo], msg.as_string())
            self.statusLastOffset = statusfo.tell()
        except Exception as e:
            logger.warn('Failed to send notification message: "%s". Disabling.', e)
            self.mailTo = None
            # Be sure to seek back to EOF to append
            self.statusFile.seek(0, 2)

    def recordResult(self, tReport):
        self.statusFile.write(str(tReport)+'\n')
        self.statusFile.flush()

    def updateStatus(self):
        status = dict(more = self.feeder.shutdown or 'More tasks.',
                      age = self.age,
                      barriers = len(self.barriers),
                      contexts = self.contexts,
                      currentReturnCode = self.currentReturnCode,
                      engines = self.engines)
        # Make changes visible via KVS.
        logger.debug('Posting status: %r', status)
        self.kvs.get('DisBatch status', False)
        self.kvs.put('DisBatch status', json.dumps(status, default=lambda o: dict([t for t in o.__dict__.items() if t[0] != 'kvs'])), b'JSON')

    class EngineProxy(object):
        def __init__(self, rank, cRank, hostname, pid, start, kvs):
            self.rank, self.cRank, self.hostname, self.pid, self.kvs  = rank, cRank, hostname, pid, kvs # No need to clone kvs, this isn't a thread.
            self.cylinders, self.age, self.assigned, self.finished, self.failed = {}, 0, 0, 0, 0
            self.start = start
            self.status = 'running'
            self.last = time.time()

        def __str__(self):
            return 'Engine %d: Context %d, Host %s, PID %d, Started at %.2f, Last hear from %.2f, Age %d, Cylinders %d, Assigned %d, Finished %d, Failed %d'%(
                self.rank, self.cRank, self.hostname, self.pid, self.start, time.time()-self.last,
                self.age, len(self.cylinders), self.assigned, self.finished, self.failed)

        def addCylinder(self, pid, pgid, ckey):
            self.cylinders[ckey] = (pid, pgid, ckey)

        def removeCylinder(self, ckey):
            self.cylinders.pop(ckey)

        def stop(self):
            if self.status == 'stopped': return
            for c in self.cylinders.values():
                self.kvs.put(c[2], ('stop', None))
            self.status = 'stopping'
            
    def run(self):
        def shutDownEngine(e):
            logger.info('Stopping engine %s', e )
            for c in e.cylinders:
                try:
                    availSlots.remove(c)
                    # Try to reclaim a slot.
                    # Not important if we fail---slots are just flow control.
                    try:
                        self.slots.get(False)
                    except Empty:
                        pass
                except ValueError:
                    logger.info('%s is busy?', c)
            e.stop()

        self.kvs.put('DisBatch status', '<Starting...>', False)

        availSlots, cRank2taskCount, cylKey2eRank, finishedTasks, noMore, pending, retired = [], DD(int), {}, {}, False, DD(list), -1

        while 1:
            logger.debug('Driver loop: Age %d, Finished %d, Retired %d, Available %d, Pending %s', self.age, self.finished, retired, len(availSlots), [(a, len(l)) for a, l in sorted(pending.items())])

            # Wait for a message.
            msg, o = self.kvs.get('.controller')

            logger.debug('Incoming msg: %s %s', msg, o)
            if msg == 'clearing barriers':
                pass
            elif msg == 'context info':
                context = o
                self.contexts[context.rank] = context
            elif msg == 'cylinder available':
                #TODO: reject if no more tasks or in shutdown?
                engineRank, cpid, cpgid, ckey = o
                cylKey2eRank[ckey] = engineRank
                self.engines[engineRank].addCylinder(cpid, cpgid, ckey)
                availSlots.append(ckey)
                self.slots.put(True)
            elif msg == 'cylinder stopped':
                engineRank, ckey = o
                logger.info('%s stopped', ckey)
                self.engines[engineRank].removeCylinder(ckey)
                try:
                    self.slots.get(False)
                except Empty:
                    pass
            elif msg == 'engine started':
                #TODO: reject if no more tasks or in shutdown?
                rank, cRank, hn, pid, start = o
                self.engines[rank] = self.EngineProxy(rank, cRank, hn, pid, start, kvs)
            elif msg == 'engine stopped':
                status, rank = o
                self.engines[rank].status = 'stopped'
                self.engines[rank].last = time.time()
                logger.info('Engine %d stopped, %s', rank, status)
            elif msg == 'feeder exception':
                logger.info('Emergency shutdown')
                break
            elif msg == 'no more tasks':
                noMore = True
                logger.info('No more tasks: %d accepted', o)
                self.barriers.append(TaskReport(TaskInfo(o, -1, -1, -1, None, None, kind='D'), start=time.time()))
            elif msg == 'register':
                which, key = o
                if which == 'context':
                    self.kvs.put(key, self.contextCount)
                    self.contextCount += 1
                elif which == 'engine':
                    self.kvs.put(key, self.engineCount)
                    self.engineCount += 1
                else:
                    logger.error('Register? %s for %s', which , key)
            elif msg == 'special task':
                tinfo = o
                finishedTasks[tinfo.taskId] = True # TODO: For barriers, set finished wehn barrier is met?
                if tinfo.kind in 'BCD':
                    logger.info('Finishing barrier %d.'%tinfo.taskId)
                    # TODO: Add assertion to verify ordering property?
                    self.barriers.append(TaskReport(tinfo, start=time.time()))
                else:
                    assert tinfo.kind == 'S'
                    self.recordResult(tReport)
            elif msg == 'stop context':
                cRank = o
                for e in self.engines.values():
                    if e.cRank != cRank:
                        continue
                    shutDownEngine(e)
            elif msg == 'stop engine':
                rank = o
                shutDownEngine(self.engines[rank])
            elif msg == 'task':
                tinfo = o
                if tinfo.kind == 'P':
                    logger.info('Posting per engine task "%s" %s', tinfo.taskKey, tinfo)
                    self.kvs.put(tinfo.taskKey, ('task', tinfo))
                else:
                    pending[tinfo.taskAge].append(tinfo)
            elif msg == 'task done':    
                tReport, engineRank, cid, cAge, ckey = o
                assert isinstance(tReport, TaskReport)
                e = self.engines[engineRank]
                e.last = time.time()
                assert e.age <= cAge
                e.age = max(e.age, cAge)
                tinfo = tReport.taskInfo
                finishedTasks[tinfo.taskId] = True

                assert tinfo.kind in 'NP'
                self.recordResult(tReport)
                # TODO: Count per engine?
                self.finished += 1
                e.finished += 1
                if tinfo.kind == 'N':
                    e.assigned -= 1
                    if e.status == 'running':
                        availSlots.append(ckey)
                        self.slots.put(True)

                if tReport.returncode:
                    self.failed += 1
                    self.engines[engineRank].failed += 1
                    assert self.barriers == [] or tReport.taskInfo.taskId < self.barriers[0].taskInfo.taskId
                    if self.currentReturnCode == 0:
                        # Remember the first failure. Somewhat arbitrary.
                        self.currentReturnCode = tReport.returncode

                # Maybe we want to track results by streamIndex instead of taskId?  But then there could be more than
                # one per key
                if self.trackResults:
                    self.kvs.put(self.trackResults%tinfo.taskId, str(tReport), False)
                if self.mailTo and self.finished%self.mailFreq == 0:
                    self.sendNotification()
            else:
                raise Exception('Weird message: ' + msg)

            if self.barriers:
                # Check if barrier is done.
                for x in range(retired+1, self.barriers[0].taskInfo.taskId):
                    if x not in finishedTasks:
                        retired = x - 1
                        break
                else:
                    # we could prune finsihedTasks at this point.
                    bReport = self.barriers.pop(0)
                    bTinfo = bReport.taskInfo
                    # TODO: add assertion to verify age?
                    retired = bTinfo.taskId
                    logger.info('Finished barrier %d: %s.', retired, bTinfo)
                    if bTinfo.kind == 'D':
                        break
                    bReport.end = time.time()
                    self.recordResult(bReport)
                    # Let the feeder know when the age changes.
                    newAge = bTinfo.taskAge + 1
                    for x in range(self.age+1, newAge+1):
                        self.ageQ.put(x)
                    self.age = newAge
                    # If user specified a KVS key, use it to signal the barrier is done.
                    if bTinfo.bKey:
                        logger.info('put %s: %d.', bTinfo.bKey, bTinfo.taskId)
                        self.kvs.put(bTinfo.bKey, str(bTinfo.taskId), False)
                    if bTinfo.kind == 'C' and self.currentReturnCode:
                        # a "check" barrier fails if any tasks before it do (since the start or the last barrier).
                        tinfo.returncode = 1
                        # stop the feeder (prompting DoneTask)
                        self.feeder.shutdown = 'Barrier check failed.'
                        break
                    # tell the engines that the barrier has been cleared.
                    logger.info('Barrier notification %s', bTinfo)
                    self.kvs.put(bTinfo.taskKey, ('barrier notification', bTinfo))
                    if self.barriers:
                        # clearing this barrier may clear the next
                        self.kvs.put('.controller', ('clearing barriers', None))
                    # Slight change: this tracks failures since start or last barrier
                    self.currentReturnCode = 0
                    bTinfo = None

            while pending[self.age] and availSlots:
                ckey = availSlots.pop(0)
                tinfo = pending[self.age].pop(0)
                logger.info('Giving %s %s', ckey, tinfo)
                self.kvs.put(ckey, ('task', tinfo))

                e = self.engines[cylKey2eRank[ckey]]
                e.assigned += 1
                cRank2taskCount[e.cRank] += 1
                limit = self.contexts[e.cRank].args.context_task_limit
                if limit and cRank2taskCount[e.cRank] == limit:
                    shutDownEngine(e)
                
            if noMore and sum([len(p) for p in pending.values()]) == 0:
                # Really nothing more to do.
                for ckey in availSlots:
                    logger.info('Notifying "%s" there is no more work.', ckey)
                    self.kvs.put(ckey, ('stop', None))
                availSlots = []
                
            # Make changes visible via KVS.
            self.updateStatus()

        logger.info('Driver done')
        self.statusFile.close()
        self.kvs.close()
        self.feeder.join()

##################################################################### ENGINE

# A simple class to count the number of bytes from a file stream (e.g., pipe),
# and possibly collect the first and/or last few bytes of it
class OutputCollector(Thread):
    def __init__(self, pipe, takeStart=0, takeEnd=0):
        super(OutputCollector, self).__init__(name='OutputCollector')
        # We don't really care for python's file abstraction -- get back a real fd
        self.pipefd = os.dup(pipe.fileno())
        pipe.close()
        self.takeStart = takeStart
        self.takeEnd = takeEnd
        self.dataStart = b''
        self.dataEnd = b''
        self.bytes = 0
        self.daemon = True
        self.start()

    def read(self, count):
        try:
            return os.read(self.pipefd, count)
        except OSError:
            return ''

    def run(self):
        start = self.takeStart
        end = self.takeEnd
        while 1:
            if start > 0:
                r = self.read(start)
                self.dataStart += r
                start -= len(r)
            else:
                r = self.read(4096)
                if end > 0:
                    if len(r) >= end:
                        self.dataEnd = r[-end:]
                    else:
                        self.dataEnd = self.dataEnd[-end+len(r):] + r
            if not r: return
            self.bytes += len(r)

    def stop(self):
        self.join(5)
        try:
            # In case someone still has the pipe open, close our end to force this thread to stop
            os.close(self.pipefd)
        except OSError:
            pass

    def __str__(self):
        s = self.dataStart
        if self.dataEnd:
            s += b'...' + self.dataEnd
        if type(s) is not str:
            # for python3... would be better to keep raw bytes
            s = s.decode('utf-8', 'ignore')
        return s

# Once a context knows the nodes that it will be adding to a disBatch
# run, it starts an engine on each node. The engine registers with the
# controller (driver) and then starts the number of cylinders
# (execution entities) specified for the node (a map of nodes to
# cylinder count is conveyed via the KVS). Each cylinder registers
# with the controller and waits for a task to be assinged to it by the
# controller. It executes the task and upon completion sends a report
# to the controller. A cylinder exits when it receives a stop message
# from the controller. The engine waits for cylinder threads to exit,
# and will itself exit when all cylinders have exited.
class EngineBlock(Thread):
    class Cylinder(Thread):
        def __init__(self, context, env, envres, ageQs, kvs, engineRank, cylinderId):
            super(EngineBlock.Cylinder, self).__init__()
            self.daemon = True
            self.context, self.ageQs, self.engineRank, self.cylinderId = context, ageQs, engineRank, cylinderId
            logger.info('Cylinder %d initializing', self.cylinderId)
            self.localEnv = env.copy()
            for v, l in envres.items():
                try:
                    self.localEnv[v] = l[0 if -1 == cylinderId else cylinderId] # Allow the per engine cylinder to access cylinder
                                                                                # 0's resources. TODO: Ensure some sort of lock?
                except IndexError:
                    # safer to set it empty than delete it for most cases
                    self.localEnv[v] = ''
            self.shuttingDown = False
            self.taskProc = None
            self.kvs = kvs.clone()
            if -1 == cylinderId:
                self.keyIndex = 0
                self.key = '.per engine %d'
            else:
                self.key = '.cylinder %d %d'%(self.engineRank, self.cylinderId)
            self.start()

        def run(self):
            logger.info('Cylinder %d in run', self.cylinderId)
            #signal.signal(signal.SIGTERM, lambda s, f: sys.exit(1))
            try:
                self.main()
            except socket.error as e:
                if not self.shuttingDown:
                    logger.info('Cylinder %d got socket error %r', self.cylinderId, e)
            except Exception as e:
                logger.exception('Cylinder %d exception: '%self.cylinderId)
            finally:
                logger.info('Cylinder %d stopping.', self.cylinderId)
                killPatiently(self.taskProc, 'cylinder %d subproc' % self.cylinderId, 2)

        def main(self):
            self.pid = os.getpid() # TODO: Remove
            self.pgid = os.getpgid(0)
            logger.info('Cylinder %d firing, %d, %d.', self.cylinderId, self.pid, self.pgid)
            if self.cylinderId != -1:
                self.kvs.put('.controller', ('cylinder available', (self.engineRank, self.pid, self.pgid, self.key)))

            age = 0
            while 1:
                #TODO: Ack Thppt
                pec = self.cylinderId == -1
                if pec:
                    key = self.key%self.keyIndex
                    self.keyIndex += 1
                    kOp = self.kvs.view
                else:
                    key = self.key
                    kOp = self.kvs.get
                logger.info('Wating for %s', key)
                msg, ti = kOp(key)
                logger.info('%s got %s %s', key, msg, ti)
                if msg == 'stop':
                    logger.info('Cylinder %d received %s, exiting.', self.cylinderId, msg)
                    self.kvs.put('.controller', ('cylinder stopped', (self.engineRank, self.key)))
                    self.shuttingDown = True
                    break

                if not pec:
                    while age != ti.taskAge:
                        newage = self.ageQs[self.cylinderId].get()
                        assert newage == age+1
                        age = newage
                        
                logger.info('Cylinder %d executing %s.', self.cylinderId, ti)
                if not pec or msg != 'barrier notification':
                    self.localEnv['DISBATCH_STREAM_INDEX'], self.localEnv['DISBATCH_REPEAT_INDEX'], self.localEnv['DISBATCH_TASKID'] = str(ti.taskStreamIndex), str(ti.taskRepIndex), str(ti.taskId)
                    t0 = time.time()
                    try:
                        self.taskProc = SUB.Popen(['/bin/bash', '-c', ti.taskCmd], env=self.localEnv, stdin=None, stdout=SUB.PIPE, stderr=SUB.PIPE, preexec_fn=os.setsid, close_fds=True)
                        pid = self.taskProc.pid
                        obp = OutputCollector(self.taskProc.stdout, 40, 40)
                        ebp = OutputCollector(self.taskProc.stderr, 40, 40)
                        r = self.taskProc.wait()
                        self.taskProc = None
                        t1 = time.time()

                        obp.stop()
                        ebp.stop()
                        tr = TaskReport(ti, self.context.node, pid, r, t0, t1, obp.bytes, str(obp), ebp.bytes, str(ebp))
                    except Exception as e:
                        self.taskProc = None
                        t1 = time.time()
                        estr = 'Exception during task execution: ' + str(e)
                        tr = TaskReport(ti, self.context.node, -1, getattr(e, 'errno', 200), t0, t1, 0, '', len(estr), estr)

                    self.kvs.put('.controller', ('task done', (tr, self.engineRank, self.cylinderId, age, self.key)))

                logger.info('Cylinder %s completed: %s', self.cylinderId, tr)
                if pec:
                    age += 1
                    for q in self.ageQs:
                        q.put(age)
                        
    def __init__(self, kvs, context, rank):
        super(EngineBlock, self).__init__(name='EngineBlock')
        self.daemon = True
        self.context = context
        self.rank = rank
        cylinders = context.cylinders[context.nodeId]

        env = os.environ
        envres = {}
        for v in context.envres:
            e = env.get(v)
            if e:
                l = e.split(',')
                if len(l) < cylinders:
                    logger.error('Requested envres variable "%s" has too few values, decreasing cylinders to match: %s', v, e)
                    # This may not be safe: driver is still feeding tasks based on original count
                    cylinders = len(l)
                elif len(l) > cylinders:
                    logger.warning('Requested envres variable "%s" has too many values, so some resources will not be used: %s', v, e)
                envres[v] = l
            else:
                logger.warning('Requested envres variable "%s" not found', v)

        self.parent = kvs
        self.kvs = kvs.clone()
        # Get rank. TODO: Convert to a request to driver in order to avoid possiblity of crashing here and hanging other engine
        # signons.
        self.rank = rank
        self.kvs.put('.controller', ('engine started', (self.rank, context.rank, myHostname, myPid, time.time())))
        env.update(self.kvs.view('.common env'))
        # Note we are using the Queue construct from the
        # mulitprocessing module---we need to coordinate between
        # independent processes.
        self.ageQs = [Queue() for x in range(cylinders)]
        self.cylinders = [self.Cylinder(context, env, envres, self.ageQs, kvs, self.rank, x) for x in range(cylinders)]
        self.pec = self.Cylinder(context, env, envres, self.ageQs, kvs, self.rank, -1)
        self.age, self.finished, self.inFlight, self.liveCylinders, self.pending = 0, 0, 0, len(self.cylinders), DD(list)
        self.kvs.put('engine %d'%self.rank, 'running')
        self.start()

    def run(self):
        try:
            for c in self.cylinders:
                c.join()
            self.kvs.put('.controller', ('engine stopped', ('OK', self.rank)))
        except Exception as e:
            logger.exception('EngineBlock')
            self.kvs.put('.controller', ('engine stopped', (str(e), self.rank)))
        finally:
            self.kvs.get('engine %d'%self.rank)
            self.kvs.put('engine %d'%self.rank, 'engine stopped')
            self.kvs.close()
            self.parent.close()


##################################################################### MAIN

if '__main__' == __name__:
    import argparse, copy

    #sys.setcheckinterval(1000000)

    if len(sys.argv) > 1 and sys.argv[1] == '--engine':
        argp = argparse.ArgumentParser(description='Task execution engine.')
        argp.add_argument('--engine', action='store_true', help='Run in execution engine mode.')
        argp.add_argument('-n', '--node', type=str, help='Name of this engine node.')
        argp.add_argument('kvsserver', help='Address of kvs sever used to relay data to this execution engine.')
        argp.add_argument('kvsKey', help='Key for my context.')
        args = argp.parse_args()
        # Stagger start randomly to throttle kvs connections
        time.sleep(random.random()*5.0)
        kvs = kvsstcp.KVSClient(args.kvsserver)
        dbInfo = kvs.view('.db info')
        rank = register(kvs, 'engine')
        context = kvs.view(args.kvsKey)
        try:
            os.chdir(dbInfo.wd)
        except Exception as e:
            print('Failed to change working directory to "%s".'%dbInfo.wd, file=sys.stderr)
        context.setNode(args.node)
        logger = logging.getLogger('DisBatch Engine')
        lconf = {'format': '%(asctime)s %(levelname)-8s %(name)-15s: %(message)s', 'level': logging.DEBUG}
        lconf['filename'] = '%s_%s_%s_engine_%d.log'%(dbInfo.uniqueId, context.label, args.node, rank)
        logging.basicConfig(**lconf)
        logger.info('Starting engine %s (%d) on %s (%d) in %s.', context.node, rank, myHostname, myPid, os.getcwd())

        e = EngineBlock(kvs, context, rank)

        def shutdown(s=None, f=None):
            # TODO: logging is not signal safe.
            logger.info('Engine shutting down.')
            for c in e.cylinders:
                if c.is_alive():
                    logger.info('forcing cylinder termination')
                    try:
                        killPatiently(c.taskProc, 'cylinder task', 2)
                    except OSError:
                        pass
            if s:
                logger.info('exiting on signal')
                sys.exit(1)
        signal.signal(signal.SIGTERM, shutdown)

        try:
            e.join()
        except socket.error as r:
            logger.info('got socket error waiting on shutdown: %r'%r)
        except Exception as e:
            logger.exception('EngineBlock during join.')
        finally:
            shutdown()
        kvs.close()
        logger.info('Remaining processes:\n' + SUB.check_output(['ps', 'fuhx', '--cols', '1000']).decode('utf-8', 'ignore'))
        sys.exit(0)
    elif len(sys.argv) > 1 and sys.argv[1] == '--context':
        argp = argparse.ArgumentParser(description='Set up disBatch execution context')
        argp.add_argument('--context', action='store_true', help='Run in execution engine mode.')
        argp.add_argument('-E', '--env-resource', metavar='VAR', action='append', help=argparse.SUPPRESS) #'Assign comma-delimited resources specified in environment VAR across tasks (count should match -t)'
        argp.add_argument('-k', '--retire-cmd', type=str, metavar='COMMAND', help="Shell command to run to retire a node (environment includes $NODE being retired, remaining $ACTIVE node list, $RETIRED node list; default based on batch system).")
        argp.add_argument('-l', '--label', type=str, metavar='COMMAND', help="Label for this context. Should be unique.")        
        argp.add_argument('--context-task-limit', type=int, metavar='COUNT', default=0, help="Shutdown after running COUNT tasks (0 => no limit).")        
        argp.add_argument('-s', '--ssh-node', type=str, action='append', metavar='HOST:COUNT', help="Run tasks over SSH on the given nodes (can be specified multiple times for additional hosts; equivalent to setting DISBATCH_SSH_NODELIST)")
        argp.add_argument('kvsserver', help='Address of kvs sever used to relay data.')
        args = argp.parse_args()
        global kvsserver
        kvsserver = args.kvsserver
        kvs = kvsstcp.KVSClient(kvsserver)
        dbInfo = kvs.view('.db info')
        rank = register(kvs, 'context')
        try:
            os.chdir(dbInfo.wd)
        except Exception as e:
            print('Failed to change working directory to "%s".'%dbInfo.wd, file=sys.stderr)

        # Try to find a batch context.
        if args.ssh_node:
            context = SSHContext(dbInfo, rank, args)
        else:
            context = probeContext(dbInfo, rank, args)
        if not context:
            print('Cannot determine batch execution environment.', file=sys.stderr)
            sys.exit(1)

        logger = logging.getLogger('DisBatch Context')
        lconf = {'format': '%(asctime)s %(levelname)-8s %(name)-15s: %(message)s', 'level': logging.INFO}
        lconf['filename'] = '%s_%s.context.log'%(dbInfo.uniqueId, context.label)
        logging.basicConfig(**lconf)
        logging.info('%s context started on %s (%d).', context.sysid, myHostname, myPid)

        # Apply lesser of -c and -t limits
        context.cylinders = [ min(int(c / dbInfo.args.cpusPerTask), dbInfo.args.tasksPerNode) for c in context.cylinders ]
        # TODO: communicate to jobs how many CPUs they have available?

        context.envres = ','.join(args.env_resource).split(',') if args.env_resource else []

        if args.retire_cmd is not None:
            context.retireCmd = args.retire_cmd

        context.launch(kvs)
        while 1:
            context.poll()
            if not context.engines:
                break
            time.sleep(1)
        context.finish()
    else:
        argp = argparse.ArgumentParser(description='Use batch resources to process a file of tasks, one task per line.')
        argp.add_argument('--fix-paths', action='store_true', help='Configure fixed path to script and modules.')
        argp.add_argument('-p', '--prefix', metavar='PATH', default=None, help='Prefix path and name for log, dbUtil, and status files (default: ./TASKFILE_JOBID).')
        argp.add_argument('-l', '--logfile', metavar='FILE', default=None, type=argparse.FileType('w'), help='Log file.')
        argp.add_argument('--mailFreq', default=None, type=int, metavar='N', help='Send email every N task completions (default: 1). "--mailTo" must be given.')
        argp.add_argument('--mailTo', metavar='ADDR', default=None, help='Mail address for task completion notification(s).')
        argp.add_argument('-c', '--cpusPerTask', metavar='N', default=1, type=float, help='Number of cores used per task; may be fractional (default: 1).')
        argp.add_argument('-t', '--tasksPerNode', metavar='N', default=float('inf'), type=int, help='Maximum concurrently executing tasks per node (up to cores/cpusPerTask).')
        argp.add_argument('-E', '--env-resource', metavar='VAR', action='append', help=argparse.SUPPRESS) #'Assign comma-delimited resources specified in environment VAR across tasks (count should match -t)'
        argp.add_argument('-g', '--gpu', action='append_const', dest='env_resource', const='CUDA_VISIBLE_DEVICES,GPU_DEVICE_ORDINAL', help='Use assigned GPU resources')
        argp.add_argument('-k', '--retire-cmd', type=str, metavar='COMMAND', help='Shell command to run to retire a node (environment includes $NODE being retired, remaining $ACTIVE node list, $RETIRED node list; default based on batch system). Incompatible with "--ssh-node".')
        argp.add_argument('-K', '--no-retire', dest='retire_cmd', action='store_const', const='', help="Don't retire nodes from the batch system (e.g., if running as part of a larger job); equivalent to -k ''.")
        argp.add_argument('-s', '--ssh-node', type=str, action='append', metavar='HOST:COUNT', help="Run tasks over SSH on the given nodes (can be specified multiple times for additional hosts; equivalent to setting DISBATCH_SSH_NODELIST)")
        argp.add_argument('-S', '--startup-only', action='store_true', help='Startup only the disBatch server (and KVS server if appropriate). Use "dbUtil..." script to add execution contexts. Incompatible with "--ssh-node".') #TODO: Add addDBExecContext file name override?
        argp.add_argument('-r', '--resume-from', metavar='STATUSFILE', action='append', help='Read the status file from a previous run and skip any completed tasks (may be specified multiple times).')
        argp.add_argument('-R', '--retry', action='store_true', help='With -r, also retry any tasks which failed in previous runs (non-zero return).')
        argp.add_argument('--force-resume', action='store_true', help="With -r, proceed even if task commands/lines are different.")
        argp.add_argument('-e', '--exit-code', action='store_true', help='When any task fails, exit with non-zero status (default: only if disBatch itself fails)')
        argp.add_argument('-w', '--web', action='store_true', help='Enable web interface.')
        argp.add_argument('--kvsserver', nargs='?', default=True, metavar='HOST:PORT', help='Use a running KVS server.')
        source = argp.add_mutually_exclusive_group(required=True)
        source.add_argument('--taskcommand', default=None, metavar='COMMAND', help='Tasks will come from the command specified via the KVS server (passed in the environment).')
        source.add_argument('--taskserver', nargs='?', default=False, metavar='HOST:PORT', help='Tasks will come from the KVS server.')
        source.add_argument('taskfile', nargs='?', default=None, type=argparse.FileType('r'), help='File with tasks, one task per line ("-" for stdin)')
        args = argp.parse_args()

        # A lone '--fix-paths' option is handled at the beginning of
        # this script.  Anything other invocation with '--fix-paths'
        # is an error.
        if args.fix_paths:
            print('You must use --fix-paths without any other arguments.', file=sys.stderr)
            sys.exit(1)

        if args.mailFreq and not args.mailTo:
            argp.print_help()
            sys.exit(1)
        if not args.mailFreq and args.mailTo:
            args.mailFreq = 1

        if args.startup_only and (args.env_resource or args.retire_cmd or args.ssh_node):
            argp.print_help()
            sys.exit(1)
            
        if not args.kvsserver:
            args.kvsserver = args.taskserver
        elif args.taskserver is None: # --taskserver with no argument
            args.taskserver = args.kvsserver
        elif args.taskserver and args.kvsserver != args.taskserver:
            print('Cannot use different --kvsserver and --taskservers.', file=sys.stderr)
            sys.exit(1)

        try:
            tfn = os.path.basename(args.taskfile.name).strip('<>')
        except AttributeError:
            tfn = 'STREAM'
        if args.prefix:
            uniqueId = args.prefix
        else:
            uniqueId = '%s_disBatch_%s_%03d'%(tfn, time.strftime('%y%m%d%H%M%S'), int(random.random()*1000))

        logger = logging.getLogger('DisBatch')
        lconf = {'format': '%(asctime)s %(levelname)-8s %(name)-15s: %(message)s', 'level': logging.DEBUG}
        if args.logfile:
            args.logfile.close()
            lconf['filename'] = args.logfile.name
        else:
            lconf['filename'] = uniqueId + '_driver.log'
        logging.basicConfig(**lconf)

        logger.info('Starting feeder (%d) on %s in %s.', myPid, myHostname, os.getcwd())

        if args.kvsserver is True:
            # start our own
            kvsst = kvsstcp.KVSServerThread(socket.gethostname(), 0)
            kvsserver = '%s:%d'%kvsst.cinfo
            kvsinfotxt = uniqueId + '_kvsinfo.txt'
            with open(kvsinfotxt, 'w') as kvsi:
                kvsi.write(kvsserver)
            kvsenv = kvsst.env()
        else:
            # use one given (possibly via environment)
            kvsst = None
            kvsserver = args.kvsserver
            kvsenv = None

        logger.info('KVS Server: %s', kvsserver)
        kvs = kvsstcp.KVSClient(kvsserver)
        # Make pickle compatible copy of args.
        targs = copy.copy(args)
        if args.taskfile:
            targs.taskfile = args.taskfile.name
        if args.logfile:
            targs.logfile = args.logfile.name
        dbInfo = DisBatchInfo(targs, tfn, uniqueId, os.getcwd())
        kvs.put('.db info', dbInfo)
        
        taskProcess = None
        if args.taskfile:
            taskSource = args.taskfile
        else:
            taskSource = KVSTaskSource(kvs)
            if args.taskcommand:
                taskProcess = TaskProcess(taskSource, args.taskcommand, shell=True, env=kvsenv, close_fds=True)

        tasks = taskGenerator(taskSource)

        if args.resume_from:
            tasks = statusTaskFilter(tasks, parseStatusFiles(*args.resume_from), args.retry, args.force_resume)

        if args.web:
            from kvsstcp import wskvsmu
            urlfile = uniqueId + '_url'
            wskvsmu.main(kvsserver, urlfile=open(urlfile, 'w'), monitorspec=':gpvw')

        ecfn = '%s_dbUtil.sh'%uniqueId
        dbRoot = os.path.split(DisBatchPath)[0]
        open(ecfn, 'w').write(open(dbRoot+'/dbUtil.sh', 'r').read().format(dbRoot=dbRoot,kvsserver=kvsserver,uniqueId=uniqueId))
        os.chmod(ecfn, 0o755)

        if not args.startup_only:
            # Is there a cleaner way to do this?
            extraArgs = []
            if args.env_resource:
                map(lambda x: extraArgs.extend(['--env-resource', x]), args.env_resource)
            if args.retire_cmd:   extraArgs += ['--retire-cmd', args.retire_cmd]
            if args.ssh_node:
                map(lambda x: extraArgs.extend(['--ssh-node', x]), args.ssh_node)
            
            print(repr(extraArgs))
            subContext = SUB.Popen([DisBatchPath, '--context'] + extraArgs + [kvsserver], stdin=open(os.devnull, 'r'), stdout=open(uniqueId + '_context_wrap.out', 'w'), stderr=open(uniqueId + '_context_wrap.err', 'w'), close_fds=True)
        else:
            print('Run this script to add compute contexts:\n   ' + ecfn)
            subContext = None

        driver = Driver(kvs, dbInfo, tasks, getattr(taskSource, 'resultkey', None), args.mailTo, args.mailFreq)
        try:
            while driver.isAlive():
                if taskProcess and taskProcess.r:
                    logger.warn('Task generator failed; forcing shutdown')
                    sys.exit(taskProcess.r)
                driver.join(15)
        except Exception as e:
            logger.exception('Watchdog')
        finally:
            try:
                logger.info("Shutting down")
                # TODO: Filter out engines with 0 slots (already shutdown)?
                eks = []
                for ex, e in driver.engines.items():
                    eks.append(ex)
                    e.stop()
                    
                logger.info("checking engines: %r", eks)
                for attempt in [1, 2, 3]:
                    neks = []
                    for ek in eks:
                        status = kvs.view('engine %d'%ek)
                        logger.info('engine %s reports %s.', ek, status)
                        if status != 'engine stopped':
                            neks.append(ek)
                    if not neks: break
                    logger.info('Hold outs: %r', neks)
                    eks = neks
                    time.sleep(3)
            except Exception as e:
                logger.exception('During shutdown')
            kvs.close()
            if kvsst: kvsst.shutdown()
            if args.kvsserver is True:
                try:
                    os.unlink(kvsinfotxt)
                except OSError:
                    # may have been already removed -- can happen if multiple disbatch runs in same directory
                    pass
            if args.web: os.unlink(urlfile)
            if subContext:
                # TODO: add some kind of timeout here?
                subContext.wait()
        if subContext and subContext.returncode:
            print('Some engine processes failed -- please check the logs', file=sys.stderr)
            sys.exit(1)

        if args.exit_code and f.failed:
            print('Some tasks failed with non-zero exit codes -- please check the logs', file=sys.stderr)
            sys.exit(1)
