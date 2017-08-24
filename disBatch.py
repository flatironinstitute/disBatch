#!/usr/bin/env python
import json, logging, os, random, re, signal, socket, subprocess as SUB, sys, time

from multiprocessing import Process as mpProcess, Queue as mpQueue
from Queue import Queue, Empty
from threading import BoundedSemaphore, Thread, Lock
from ast import literal_eval

DisBatchPath, ImportDir, PathsFixed = None, None, False # <= May need to set these, setting PathsFixed to True as well.

# Handle self-modification invocation early, before messing with paths and other imports
if '__main__' == __name__ and sys.argv[1:] == ["--fix-paths"]:
    import tempfile
    DisBatchPath = os.path.realpath(__file__)
    if not os.path.exists(DisBatchPath):
        print >>sys.stderr, 'Unable to find myself; set DisBatchPath and ImportDir manually at the top of disBatch.py.'
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
                    print >>sys.stderr, "Changing path info to %r"%l
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

PythonPath = os.environ.get('PYTHONPATH', '')
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
        print >>sys.stderr, 'This script is looking in the wrong place for "kvssctp". Try running "%s --fix-paths" or editing it by hand.'%DisBatchPath
    else:
        print >>sys.stderr, '''
Could not find kvsstcp. If there is a "kvsstcp" directory in "%s",
try running "%s --fix-paths". Otherwise review the installation
instructions.
'''%(ImportDir, DisBatchPath)
    sys.exit(1)
    
myHostname = socket.gethostname()
myPid = os.getpid()

# Note that even though these are case insensitive, only lines that start with upper-case '#DISBATCH' prefixes are tested
dbbarrier = re.compile('^#DISBATCH BARRIER(?: (.+)?)?$', re.I)
dbcomment = re.compile('^\s*(#|$)')
dbprefix  = re.compile('^#DISBATCH PREFIX (.*)$', re.I)
dbrepeat  = re.compile('^#DISBATCH REPEAT\s+(?P<repeat>[0-9]+)(?:\s+start\s+(?P<start>[0-9]+))?(?:\s+step\s+(?P<step>[0-9]+))?(?: (?P<command>.+))?\s*$', re.I)
dbsuffix  = re.compile('^#DISBATCH SUFFIX (.*)$', re.I)
dbpernode = re.compile('^#DISBATCH PERNODE (.*)$', re.I)

# Special ID for "out of band" task events
TaskIdOOB = -1
CmdPoison = '!!Poison!!'

def compHostnames(h0, h1):
    return h0.split('.', 1)[0] == h1.split('.', 1)[0]

def logfile(context, suffix=''):
    '''Standardized file path construction for log files.'''
    f = "%s_%s"%(getattr(context, 'name', 'disBatch'), context.jobid)
    if hasattr(context, 'node'):
        f += "_%s"%context.node
    if suffix:
        f += "_%s"%suffix
    return f

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

class BatchContext(object):
    def __init__(self, sysid, jobid, nodes, cylinders):
        self.sysid, self.jobid, self.nodes, self.cylinders = sysid, jobid, nodes, cylinders
        self.wd = os.getcwd() #TODO: Easy enough to override, but still... Is this the right place for this?
        self.error = False # engine errors (non-zero return values)

    def __str__(self):
        return 'Batch system: %s\nJobID: %s\nNodes: %r\nCylinders: %r\n'%(self.sysid, self.jobid, self.nodes, self.cylinders)

    def launch(self, kvs):
        '''Launch the engine processes on all the nodes by calling launchNode for each.'''
        kvs.put('.context', self)
        self.engines = dict() # live subprocesses
        for n in self.nodes:
            self.engines[n] = self.launchNode(n)

    def poll(self):
        '''Check if any engines have stopped.'''
        for n, e in self.engines.items():
            r = e.poll()
            if r is not None:
                logger.info('Engine %s exited: %d', n, r)
                del self.engines[n]
                self.retireNode(n, r)

    def launchNode(self, node):
        '''Launch an engine for a single node.  Should return a subprocess handle (unless launch itself is overridden).'''
        raise NotImplementedError('%s.launchNode is not implemented' % type(self))

    def retireNode(self, node, ret, msg='no-op: add clean up hook here?'):
        '''Called when a node has exited.  May be overridden to release resources.'''
        if ret: self.error = True
        logger.info('Retiring node "%s": %s', node, msg)

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
    flat = []
    prefix = None
    # break up the node list into: non-ranges, ranges, non-ranges, ...
    # commas in the non-ranges separate hosts, the last of which is the prefix for the following ranges.
    for x in re.split(r'(\[.*?\])', nl):
        if x == '': continue
        if x[0] == '[':
            x = x[1:-1]
            for r in x.split(','):
                lo = hi = r
                if '-' in r:
                    lo, hi = r.split('-')
                fmt = '%s%%0%dd'%(prefix, max(len(lo), len(hi)))
                for x in range(int(lo), int(hi)+1): flat.append(fmt%x)
            prefix = None
        else:
            if prefix: flat.append(prefix)
            pp = x.split(',')
            [flat.append(p) for p in pp[:-1] if p]
            prefix = pp[-1]
    if prefix: flat.append(prefix)
    return flat

class SlurmContext(BatchContext):
    def __init__(self):
        jobid = os.environ['SLURM_JOBID']
        nodes = nl2flat(os.environ['SLURM_NODELIST'])

        cylinders = []
        for tr in os.environ['SLURM_TASKS_PER_NODE'].split(','):
            m = re.match(r'([^\(]+)(?:\(x([^\)]+)\))?', tr)
            c, m = m.groups()
            if m == None: m = '1'
            cylinders += [int(c)]*int(m)

        self.driverNode = None
        super(SlurmContext, self).__init__('SLURM', jobid, nodes, cylinders)

    def launchNode(self, n):
        return SUB.Popen(['srun', '-N', '1', '-n', '1', '-w', n, DisBatchPath, '--engine', '-n', n, kvsserver], close_fds=True)

    def retireNode(self, node, ret):
        if compHostnames(node, myHostname):
            super(SlurmContext, self).retireNode(node, ret, 'refusing to retire driver node "%s".' % myHostname)
            self.driverNode = node
        else:
            super(SlurmContext, self).retireNode(node, ret, "updating node list")
            nodes = ','.join(self.engines.iterkeys())
            if self.driverNode: nodes += ',' + self.driverNode
            command = ['scontrol', 'update', 'JobId=%s'%self.jobid, 'NodeList='+nodes]
            logger.debug("Retirement: %s", repr(command))
            try:
                SUB.check_call(command, close_fds=True)
            except Exception, e:
                logger.warn('Retirement planning needs improvement: %s', repr(e))

    def setNode(self, node=None):
        super(SlurmContext, self).setNode(node or os.environ.get('SLURMD_NODENAME'))

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
    def __init__(self):
        jobid = os.environ.get('DISBATCH_SSH_JOBID', '%d_%.6f'%(myPid, time.time()))

        cylinders, nodes = [], []
        for p in os.environ['DISBATCH_SSH_NODELIST'].split(','):
            n, e = p.split(':')
            if n == 'localhost': n = myHostname
            nodes.append(n)
            cylinders.append(int(e))

        super(SSHContext, self).__init__('SSH', jobid, nodes, cylinders)

    def launchNode(self, n):
        prefix = [] if compHostnames(n, myHostname) else ['ssh', n, 'PYTHONPATH=' + PythonPath]
        return SUB.Popen(prefix + [DisBatchPath, '--engine', '-n', n, kvsserver], stdin=open(os.devnull, 'r'), stdout=open(logfile(self, '%s_engine_wrap.out'%n), 'w'), stderr=open(logfile(self, '%s_engine_wrap.err'%n), 'w'), close_fds=True)

def probeContext():
    if 'SLURM_JOBID' in os.environ: return SlurmContext()
    #if ...: return GEContext()
    #if ...: LSFContext()
    #if ...: PBSContext()
    if 'DISBATCH_SSH_NODELIST' in os.environ: return SSHContext()


class TaskInfo(object):
    def __init__(self, taskId, taskStreamIndex, taskRepIndex, taskCmd, taskKey, host = '', pid = 0, returncode = 0, start = 0, end = 0, outbytes = 0, outdata = '', errbytes = 0, errdata = '', skip = False):
        self.taskId, self.taskStreamIndex, self.taskRepIndex, self.taskCmd, self.taskKey = taskId, taskStreamIndex, taskRepIndex, taskCmd, taskKey
        self.host, self.pid, self.returncode, self.start, self.end, self.outbytes, self.outdata, self.errbytes, self.errdata = host, pid, returncode, start, end, outbytes, outdata, errbytes, errdata
        self.skip = skip

    def flags(self):
        return (  ('R' if self.returncode else ' ')
                + ('O' if self.outbytes   else ' ')
                + ('E' if self.errbytes   else ' ')
                + ('S' if self.skip       else ''))

    def __str__(self):
        # If this changes, update parseStatusFile below and disBatcher.py too
        return '\t'.join([str(x) for x in [self.flags(), self.taskId, self.taskStreamIndex, self.taskRepIndex, self.host, self.pid, self.returncode, self.end - self.start, self.start, self.end, self.outbytes, repr(self.outdata), self.errbytes, repr(self.errdata), repr(self.taskCmd)]])

    def __eq__(self, other):
        return type(self) is type(other) and self.taskId == other.taskId and self.taskStreamIndex == other.taskStreamIndex and self.taskRepIndex == other.taskRepIndex and self.taskCmd == other.taskCmd # and self.taskKey == other.taskKey

    def __ne__(self, other):
        return not self == other

class BarrierTask(TaskInfo):
    def __init__(self, taskId, taskStreamIndex, taskRepIndex, taskCmd, key=None, check=False, returncode=0, errdata=''):
        super(BarrierTask, self).__init__(taskId, taskStreamIndex, taskRepIndex, taskCmd, key, myHostname, myPid, returncode=returncode, errdata=errdata)
        self.check = check

    def flags(self):
        return 'B'

class DoneTask(BarrierTask):
    '''Implicit barrier posted when there are no more tasks.'''
    def __init__(self, taskId, taskStreamIndex, err=''):
        super(DoneTask, self).__init__(taskId, taskStreamIndex, -1, '', 'done!', returncode=1 if err else 0, errdata=err)

    def flags(self):
        return 'D'

def parseStatusFile(f):
    status = dict()
    with open(f, 'r') as s:
        for l in s:
            d = l.split('\t')
            if len(d) != 15: raise Exception('Invalid status line: %r'%l)
            if d[0] in 'BD': continue
            status[int(d[1])] = TaskInfo(int(d[1]), int(d[2]), int(d[3]), literal_eval(d[14]), '.task', d[4], int(d[5]), int(d[6]), float(d[8]), float(d[9]), int(d[10]), literal_eval(d[11]), int(d[12]), literal_eval(d[13]), True)
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

    def next(self):
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
def taskGenerator(tasks, context):
    tsx = 0 # "line number" of current task
    taskCounter = 0 # next taskId
    prefix = suffix = ''

    while 1:
        tsx += 1
        try:
            t = tasks.next()
        except StopIteration:
            # Signals there will be no more tasks.
            break

        logger.debug('Task: %s', t)

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
                        yield TaskInfo(taskCounter, tsx, rx, cmd, '.task')
                        taskCounter += 1
                        rx += step
                        repeats -= 1
                    continue
                m = dbpernode.match(t)
                if m:
                    cmd = m.group(1)
                    for rx, node in enumerate(context.nodes):
                        yield TaskInfo(taskCounter, tsx, rx, prefix + cmd + suffix, '.node.'+node)
                        taskCounter += 1
                    continue
                m = dbbarrier.match(t)
                if m:
                    yield BarrierTask(taskCounter, tsx, -1, t, m.group(1))
                    taskCounter += 1
                    continue
                logger.error('Unknown #DISBATCH directive: %s', t)

            if dbcomment.match(t):
                # Comment or empty line, ignore
                continue

            yield TaskInfo(taskCounter, tsx, -1, prefix + t + suffix, '.task')
            taskCounter += 1

    logger.info('Processed %d tasks.', taskCounter)
    yield DoneTask(taskCounter, tsx)

def statusTaskFilter(tasks, status):
    while True:
        t = tasks.next()
        s = status.get(t.taskId)
        # optionally check for that status info matches:
        if s and s != t: raise Exception('Recovery status file task mismatch:\n%s\n%s' % (s, t))
        if s == t and s.returncode == 0:
            # skip
            yield s
        else:
            yield t

# Main control loop that sends new tasks to the execution engines.
class Feeder(Thread):
    def __init__(self, kvs, context, tasks, taskSlots):
        super(Feeder, self).__init__(name='Feeder')
        self.daemon = True
        self.context = context
        self.taskGenerator = tasks
        self.taskSlots = taskSlots
        self.kvs = kvs.clone()
        # just used for status reporting (not thread safe):
        self.done = False
        self.barrier = None
        self.start()

    def run(self):
        try:
            self.main()
        except Exception, e:
            logger.error('Feeder error: %s', e)
            self.kvs.put('.finished task', DoneTask(-1, 0, str(e)))
            raise

    def main(self):
        self.kvs.put('.common env', {'DISBATCH_JOBID': str(self.context.jobid), 'DISBATCH_NAMETASKS': self.context.name}) #TODO: Add more later?
        while True:
            # Wait for a slot
            self.taskSlots.acquire()

            if self.done:
                tinfo = DoneTask(-1, 0, 'aborted')
            else:
                # Request the next task
                tinfo = self.taskGenerator.next()

            if tinfo.skip:
                self.kvs.put('.finished task', tinfo)
                continue

            if isinstance(tinfo, BarrierTask):
                if isinstance(tinfo, DoneTask):
                    self.done = True
                    # Post the poison pill. This may trigger retirement of engines.
                    self.kvs.put('.task', [TaskIdOOB, -1, -1, CmdPoison])

                self.barrier = tinfo.taskKey or True
                # Enter a barrier. We'll exit when all tasks
                # issued to this point have completed.
                tinfo.start = time.time()
                logger.info('Entering barrier (key is %s).', repr(tinfo.taskKey))

                # Wait for all the other task slots to be free (_initial_value is totalSlots)
                # These ar ethen released by Driver when barrier is finished
                for i in range(self.taskSlots._initial_value-1):
                    self.taskSlots.acquire()

                # Completed all tasks up to, but not including the barrier. Now complete the barrier.
                logger.info('Finishing barrier.')
                # post a task finished for the current barrier, just like any other task, triggering the release of all taskSlots
                tinfo.end = time.time()
                self.kvs.put('.finished task', tinfo)
                self.barrier = None

                if isinstance(tinfo, DoneTask): break
                continue

            # At this point, we have a task
            tpayload = [tinfo.taskId, tinfo.taskStreamIndex, tinfo.taskRepIndex, tinfo.taskCmd]
            logger.info('Posting task: %r', tpayload)
            self.kvs.put(tinfo.taskKey, tpayload)

        self.kvs.close()

# Main control loop that processes completed tasks.
class Driver(Thread):
    def __init__(self, kvs, context, tasks, trackResults=None, mailTo=None, mailFreq=1):
        super(Driver, self).__init__(name='Driver')
        self.context = context
        self.kvs = kvs.clone()
        self.mailTo = mailTo
        self.mailFreq = mailFreq

        self.trackResults = trackResults
        self.taskSlots = BoundedSemaphore(sum(self.context.cylinders))
        self.feeder = Feeder(self.kvs, context, tasks, self.taskSlots)

        self.finished = 0
        self.failed = 0

        self.failureFile = logfile(self.context, 'failed.txt')
        self.statusFile = open(logfile(self.context, 'status.txt'), 'w+')
        self.statusLastOffset = self.statusFile.tell()

        self.daemon = True
        self.start()

    def sendNotification(self):
        try:
            import smtplib
            from email.mime.text import MIMEText
            self.statusFile.seek(self.statusLastOffset)
            msg = MIMEText('Last %d:\n\n'%self.mailFreq + self.statusFile.read())
            msg['Subject'] = '%s (%s) has completed %d tasks'%(self.context.name, self.context.jobid, self.finished)
            if self.failed:
                msg['Subject'] += ' (%d failed)'%self.failed
            msg['From'] = self.mailTo
            msg['To'] = self.mailTo
            s = smtplib.SMTP()
            s.connect()
            s.sendmail([self.mailTo], [self.mailTo], msg.as_string())
            self.statusLastOffset = statusfo.tell()
        except Exception, e:
            logger.warn('Failed to send notification message: "%s". Disabling.', e)
            self.mailTo = None
            # Be sure to seek back to EOF to append
            self.statusFile.seek(0, 2)

    def updateStatus(self):
        status = dict(more = not self.feeder.done, barrier = self.feeder.barrier,
                finished = self.finished, failed = self.failed,
                # base active on semaphore value (correct except for barriers)
                active = self.taskSlots._initial_value - self.taskSlots._Semaphore__value)
        # Make changes visible via KVS.
        logger.debug('Posting status: %r', status)
        self.kvs.get('DisBatch status', False)
        self.kvs.put('DisBatch status', json.dumps(status, default=repr), 'JSON')

    def run(self):
        self.kvs.put('DisBatch status', '<Starting...>', False)
        while 1:
            logger.debug('Driver loop: %d', self.finished)

            # Wait for a finished task
            tinfo = self.kvs.get('.finished task')
            logger.debug('Finished task: %s', tinfo)

            if isinstance(tinfo, BarrierTask):
                if isinstance(tinfo, DoneTask): break
                # Complete the barrier task itself, exit barrier mode.
                logger.info('Finished barrier.')
                if tinfo.check and self.failed:
                    # a "check" barrier fails if any tasks before it do
                    tinfo.returncode = 1
                    # stop the feeder (prompting DoneTask)
                    self.feeder.done = True
                # If user specified a KVS key, use it to signal the barrier is done.
                elif tinfo.taskKey:
                    logger.info('put %s: %d.', tinfo.taskKey, tinfo.taskId)
                    self.kvs.put(tinfo.taskKey, str(tinfo.taskId), False)
                # Release the rest of the slots (_initial_value is totalSlots)
                for i in range(self.taskSlots._initial_value-1):
                    self.taskSlots.release()

            self.taskSlots.release()
            self.finished += 1
            self.statusFile.write(str(tinfo)+'\n')
            self.statusFile.flush()

            if tinfo.returncode:
                self.failed += 1
                with open(self.failureFile, 'a') as f:
                    if tinfo.taskRepIndex >= 0:
                        f.write("#DISBATCH REPEAT 1 start %d " % tinfo.taskRepIndex)
                    f.write(tinfo.taskCmd+'\n')

            # Maybe we want to track results by streamIndex instead of taskId?  But then there could be more than one per key.
            if self.trackResults: self.kvs.put(self.trackResults%tinfo.taskId, str(tinfo), False)
            if self.mailTo and finished%self.mailFreq == 0:
                self.sendNotification()

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
        self.pipe = pipe
        self.takeStart = takeStart
        self.takeEnd = takeEnd
        self.dataStart = ''
        self.dataEnd = ''
        self.bytes = 0
        self.daemon = True
        self.start()

    def run(self):
        start = self.takeStart
        end = self.takeEnd
        while 1:
            if start > 0:
                r = self.pipe.read(start)
                self.dataStart += r
                start -= len(r)
            else:
                r = self.pipe.read(4096)
                if end > 0:
                    if len(r) >= end:
                        self.dataEnd = r[-end:]
                    else:
                        self.dataEnd = self.dataEnd[-end+len(r):] + r
            if not r: return
            self.bytes += len(r)

    def __str__(self):
        s = self.dataStart
        if self.dataEnd:
            s += '...' + self.dataEnd
        return s

# Once we know the nodes participating in the run, we start an engine
# on each node. The engine in turn starts the number of cylinders
# (execution entities) specified for the node (a map of nodes to
# cylinder count is conveyed via the KVS). Each cylinder executes one
# task at a time. The Injector waits for an available cylinder and
# then waits for a task (grabbing a task before a cylinder is ready
# would prevent an idle cylinder of another engine from performing the
# task). The task is passed to the run method via a queue. The run
# method uses queues to feed tasks to the cylinders and accept
# results from them. It tracks work in progress and notes when the
# "poison" task has been received. This indicates no more tasks will
# be coming. At this point, once all work in progress is done, the
# engine can retire.
class EngineBlock(Thread):
    class Cylinder(mpProcess):
        def __init__(self, context, commonEnv, ciq, coq, cylinderId):
            super(EngineBlock.Cylinder, self).__init__()
            self.daemon = True
            self.context, self.commonEnv, self.ciq, self.coq, self.cylinderId = context, commonEnv, ciq, coq, cylinderId
            self.taskProc = None
            self.start()

        def run(self):
            signal.signal(signal.SIGTERM, lambda s, f: sys.exit(1))
            try:
                self.main()
            finally:
                killPatiently(self.taskProc, 'cylinder %d subproc' % self.cylinderId, 2)

        def main(self):
            self.pgid = os.getpgid(0)
            logger.info('Cylinder %d firing, %d, %d.', self.cylinderId, self.pid, self.pgid)
            baseEnv = os.environ.copy()
            baseEnv.update(self.commonEnv)
            while 1:
                (taskId, taskStreamIndex, taskRepIndex, taskCmd), throttled = self.ciq.get()
                if taskId == TaskIdOOB:
                    logger.info('Cylinder %d stopping.', self.cylinderId)
                    self.coq.put(('done', 'stopped', False))
                    break
                t0 = time.time()
                logger.info('Cylinder %d executing %s.', self.cylinderId, repr([taskId, taskStreamIndex, taskRepIndex, taskCmd]))
                baseEnv['DISBATCH_STREAM_INDEX'], baseEnv['DISBATCH_REPEAT_INDEX'], baseEnv['DISBATCH_TASKID'] = str(taskStreamIndex), str(taskRepIndex), str(taskId)
                # TODO check close fds
                self.taskProc = SUB.Popen(['/bin/bash', '-c', taskCmd], env=baseEnv, stdin=None, stdout=SUB.PIPE, stderr=SUB.PIPE, preexec_fn=os.setsid, close_fds=True)
                pid = self.taskProc.pid
                obp = OutputCollector(self.taskProc.stdout, 40, 40)
                ebp = OutputCollector(self.taskProc.stderr, 40, 40)
                r = self.taskProc.wait()
                self.taskProc = None
                t1 = time.time()
                ti = TaskInfo(taskId, taskStreamIndex, taskRepIndex, taskCmd, '.finished task', self.context.node, pid, r, t0, t1, obp.bytes, str(obp), ebp.bytes, str(ebp))
                logger.info('Cylinder %s completed: %s', self.cylinderId, ti)
                self.coq.put(('done', ti, throttled))

    class Injector(Thread):
        def __init__(self, kvs, key, throttle, fuelline):
            super(EngineBlock.Injector, self).__init__(name='Injector')
            self.daemon = True
            self.kvs = kvs.clone()
            self.key = key
            self.throttle = throttle
            self.fuelline = fuelline
            self.start()

        def run(self):
            while 1:
                if self.throttle: self.throttle.acquire()
                ti = self.kvs.get(self.key)
                logger.debug('Injector got task %s', repr(ti))
                self.fuelline.put(('task', ti, bool(self.throttle)))

    def __init__(self, kvs, context):
        super(EngineBlock, self).__init__(name='EngineBlock')
        self.daemon = True
        self.context = context
        cylinders = context.cylinders[context.nodeId]

        self.parent = kvs
        self.kvs = kvs.clone()
        self.commonEnv = self.kvs.view('.common env')
        # Note we are using the Queue construct from the
        # mulitprocessing module---we need to coordinate between
        # independent processes.
        self.ciq, self.coq, self.throttle = mpQueue(), mpQueue(), BoundedSemaphore(cylinders)
        self.Injector(kvs, ".node." + context.node, None, self.coq)
        self.Injector(kvs, ".task", self.throttle, self.coq)
        self.cylinders = [self.Cylinder(context, self.commonEnv, self.ciq, self.coq, x) for x in range(cylinders)]
        self.start()

    def run(self):
        try:
            self.main()
        finally:
            self.parent.close()

    def main(self):
        inFlight, liveCylinders = 0, len(self.cylinders)
        while liveCylinders:
            tag, o, throttled = self.coq.get()
            logger.info('Run loop: %d %d %s %s', inFlight, liveCylinders, tag, repr(o))
            if tag == 'done':
                inFlight -= 1
                if throttled: self.throttle.release()
                if o == 'stopped':
                    liveCylinders -= 1
                else:
                    self.kvs.put(o.taskKey, o)
            elif tag == 'task':
                # Handle control messages (TaskIdOOB). The default at the moment
                # is to put it back so other engines will see it. In
                # the future we may have additional codings, perhaps
                # ones that shouldn't auto propagate, so this sort of
                # test will become a bit more complicated.
                if o[0] == TaskIdOOB: self.kvs.put('.task', o)
                self.ciq.put((o, throttled))
                inFlight += 1
            else:
                logger.error('Unknown cylinder input tag: "%s" (%s)', tag, repr(o))
        self.kvs.close()

##################################################################### MAIN

if '__main__' == __name__:
    import argparse

    #sys.setcheckinterval(1000000)

    if len(sys.argv) > 1 and sys.argv[1] == '--engine':
        argp = argparse.ArgumentParser(description='Task execution engine.')
        argp.add_argument('--engine', action='store_true', help='Run in execution engine mode.')
        argp.add_argument('-n', '--node', type=str, help='Name of this engine node.')
        argp.add_argument('kvsserver', help='Address of kvs sever used to relay data to this execution engine.')
        args = argp.parse_args()
        # Stagger start randomly to throttle kvs connections
        time.sleep(random.random()*5.0)
        kvs = kvsstcp.KVSClient(args.kvsserver)
        context = kvs.view('.context')
        try:
            os.chdir(context.wd)
        except Exception, e:
            print >>sys.stderr, 'Failed to change working directory to "%s".'%context.wd
        context.setNode(args.node)
        logger = logging.getLogger('DisBatch Engine')
        lconf = {'format': '%(asctime)s %(levelname)-8s %(name)-15s: %(message)s', 'level': logging.INFO}
        lconf['filename'] = logfile(context, 'engine.log')
        logging.basicConfig(**lconf)
        logger.info('Starting engine %s (%d) on %s (%d) in %s.', context.node, context.nodeId, myHostname, myPid, os.getcwd())

        e = EngineBlock(kvs, context)
        try:
            kvs.view('.shutdown')
            logger.info('got shutdown')
        except socket.error:
            pass
        finally:
            logger.info('Engine shutting down.')
            for c in e.cylinders:
                if c.is_alive():
                    try:
                        c.terminate()
                    except OSError:
                        pass
        kvs.close()

    else:
        argp = argparse.ArgumentParser(description='Use batch resources to process a file of tasks, one task per line.')
        argp.add_argument('--fix-paths', action='store_true', help='Configure fixed path to script and modules.')
        argp.add_argument('-l', '--logfile', default=None, type=argparse.FileType('w'), help='Log file.')
        argp.add_argument('--mailFreq', default=None, type=int, metavar='N', help='Send email every N task completions (default: 1). "--mailTo" must be given.')
        argp.add_argument('--mailTo', default=None, help='Mail address for task completion notification(s).')
        argp.add_argument('-c', '--cpusPerTask', default=1, type=float, help='Number of cores used per task; may be fractional (default: 1).')
        argp.add_argument('-t', '--tasksPerNode', default=float('inf'), type=int, help='Maximum concurrently executing tasks per node (up to cores/cpusPerTask).')
        argp.add_argument('-r', '--resume-from', metavar='STATUSFILE', help='Read the given status file from a previous run and skip any sucessful tasks.')
        argp.add_argument('--web', action='store_true', help='Enable web interface.')
        argp.add_argument('--kvsserver', nargs='?', default=True, metavar='HOST:PORT', help='Use a running KVS server.')
        source = argp.add_mutually_exclusive_group(required=True)
        source.add_argument('--taskcommand', default=None, metavar='COMMAND', help='Tasks will come from the command specified via the KVS server (passed in the environment).')
        source.add_argument('--taskserver', nargs='?', default=False, metavar='HOST:PORT', help='Tasks will come from the KVS server.')
        source.add_argument('taskfile', nargs='?', default=None, type=argparse.FileType('r'), help='File with tasks, one task per line.')
        args = argp.parse_args()

        if args.fix_paths:
            print >>sys.stderr, 'You must use --fix-paths without any other arguments.'
            sys.exit(1)

        if args.mailFreq and not args.mailTo:
            argp.print_help()
            sys.exit(1)
        if not args.mailFreq and args.mailTo:
            args.mailFreq = 1

        if not args.kvsserver:
            args.kvsserver = args.taskserver
        elif args.taskserver is None: # --taskserver with no argument
            args.taskserver = args.kvsserver
        elif args.taskserver and args.kvsserver != args.taskserver:
            print >>sys.stderr, 'Cannot use different --kvsserver and --taskservers.'
            sys.exit(1)

        # Try to find a batch context.
        context = probeContext()
        if not context:
            print >>sys.stderr, 'Cannot determine batch execution environment.'
            sys.exit(1)

        # Apply lesser of -c and -t limits
        context.cylinders = [ min(int(c / args.cpusPerTask), args.tasksPerNode) for c in context.cylinders ]
        # TODO: communicate to jobs how many CPUs they have available?

        logger = logging.getLogger('DisBatch')
        lconf = {'format': '%(asctime)s %(levelname)-8s %(name)-15s: %(message)s', 'level': logging.INFO}
        if args.logfile:
            args.logfile.close()
            lconf['filename'] = args.logfile.name
        else:
            lconf['filename'] = logfile(context, 'driver.txt')
        logging.basicConfig(**lconf)

        logger.info('Starting feeder (%d) on %s in %s.', myPid, myHostname, os.getcwd())
        logger.info('Context: %s', context)

        if args.kvsserver is True:
            # start our own
            kvsst = kvsstcp.KVSServerThread(socket.gethostname(), 0)
            kvsserver = '%s:%d'%kvsst.cinfo
            with open('kvsinfo.txt', 'w') as kvsi:
                kvsi.write(kvsserver)
            kvsenv = kvsst.env()
        else:
            # use one given (possibly via environment)
            kvsst = None
            kvsserver = args.kvsserver
            kvsenv = None

        logger.info('KVS Server: %s', kvsserver)
        kvs = kvsstcp.KVSClient(kvsserver)

        taskProcess = None
        if args.taskfile:
            taskSource = args.taskfile
        else:
            taskSource = KVSTaskSource(kvs)
            if args.taskcommand:
                taskProcess = TaskProcess(taskSource, args.taskcommand, shell=True, env=kvsenv, close_fds=True)

        tasks = taskGenerator(taskSource, context)

        if args.resume_from:
            tasks = statusTaskFilter(tasks, parseStatusFile(args.resume_from))

        siglock = Lock()
        def sigChild(s, f):
            # signal handlers need to be reentrant
            global sigact
            sigact = True
            if not siglock.acquire(False): return
            try:
                while sigact:
                    sigact = False
                    context.poll()
                    if taskProcess: taskProcess.poll()
            finally:
                siglock.release()
        signal.signal(signal.SIGCHLD, sigChild)

        # Could reorder initialization some to pass this as argument
        context.name = os.path.basename(taskSource.name)

        if args.web:
            from kvsstcp import wskvsmu
            urlfile = logfile(context, 'url')
            wskvsmu.main(kvsserver, urlfile=open(urlfile, 'w'), monitorspec=':gpvw')

        context.launch(kvs)

        f = Driver(kvs, context, tasks, getattr(taskSource, 'resultkey', None), args.mailTo, args.mailFreq)
        try:
            while f.isAlive():
                if not context.engines:
                    logger.warn('All engines terminated; shutting down')
                    break
                if taskProcess and taskProcess.r:
                    logger.warn('Task generator failed; forcing shutdown')
                    sys.exit(taskProcess.r)
                f.join(60)
        finally:
            try:
                logger.info("posting .shutdown")
                kvs.put('.shutdown', '', False)
            except:
                pass
            kvs.close()
            r = context.finish()
            if kvsst: kvsst.shutdown()

        if not r:
            print >>sys.stderr, 'Some engine processes failed -- please check the logs'
            sys.exit(1)

