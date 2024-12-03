import argparse
import copy
import importlib.resources
import json
import logging
import os
import pickle
import random
import re
import signal
import socket
import subprocess as SUB
import sys
import time
import traceback
import warnings
from ast import literal_eval
from collections import defaultdict as DD
from functools import partial
from queue import Empty, Queue
from threading import Thread

from . import kvsstcp

# This is a global that will be set once the disBatch starts.
# It refers to the per-job dbUtil.sh script that is filled in with the connection info.
DbUtilPath = None

myHostname = socket.gethostname()
myPid = os.getpid()

# Note that even though these are case insensitive, only lines that start with upper-case '#DISBATCH' prefixes are tested
# fmt: off
dbbarrier   = re.compile(rb'^#DISBATCH BARRIER(?:(?: )(CHECK))?(?:(?: )(.*)$)?')
dbcomment   = re.compile(rb'^\s*(#|$)')
dbprefix    = re.compile(rb'^#DISBATCH PREFIX (.*)$', re.I)
dbrepeat    = re.compile(rb'^#DISBATCH REPEAT\s+(?P<repeat>[0-9]+)(?:\s+start\s+(?P<start>[0-9]+))?(?:\s+step\s+(?P<step>[0-9]+))?(?: (?P<command>.+))?\s*$', re.I)
dbsuffix    = re.compile(rb'^#DISBATCH SUFFIX (.*)$', re.I)
dbperengine = re.compile(rb'^#DISBATCH PERENGINE (START|STOP) (.*)$', re.I)
# fmt: on

# Heart beat info.
PulseTime = 30
NoPulse = (
    4 * PulseTime + 1
)  # A task is considered dead if we don't hear from it after 4 heart beat cycles. Counting is approximate because of the interaction of various timeouts.

logger = logging.getLogger('DisBatch')
warnings.formatwarning = lambda msg, cat, *args, **kwargs: f'{cat.__name__}: {msg}\n'  # a friendlier warning format


def compHostnames(h0, h1):
    return h0.split('.', 1)[0] == h1.split('.', 1)[0]


def waitForIt(fn, mode, loops=60):
    for x in range(loops):
        try:
            return open(fn, mode)
        except FileNotFoundError:
            time.sleep(0.5)
    else:
        raise Exception(f'Gave up waiting for {fn} after {loops} attempts.')


def waitTimeout(sub, timeout, interval=1):
    r = sub.poll()
    while r is None and timeout > 0:
        time.sleep(interval)
        timeout -= interval
        r = sub.poll()
    return r


def killPatiently(sub, name, timeout=15):
    if not sub:
        return
    r = sub.poll()
    waited = False
    if r is None:
        logger.info('Waiting for %s to finish...', name)
        waited = True
        r = waitTimeout(sub, timeout)
    if r is None:
        logger.warning('Terminating %s...', name)
        try:
            sub.terminate()
        except OSError:
            pass
        r = waitTimeout(sub, timeout)
    if r is None:
        logger.warning('Killing %s.', name)
        try:
            sub.kill()
        except OSError:
            pass
        r = sub.wait()
    if r or waited:
        logger.info('%s returned %d', name, r)
    return r


def register(kvs, which):
    # Contact the controller to be assigned an identifier via a random
    # key.
    key = str(int(10e7 * random.random()))
    kvs.put('.controller', ('register', (which, key)))
    return kvs.get(key)


class DisBatcher:
    """Encapsulates a disBatch instance."""

    def __init__(self, tasksname='DisBatcher', args=[], kvsserver=None):
        if kvsserver is None:
            # Start disBatch in a thread.
            # disBatch in turn will start KVS, we use this Queue to
            # get the connection info for the KVS server.
            kvsq = Queue()
            save_sa = sys.argv
            sys.argv = [sys.argv[0] + '.disBatch', '--taskcommand', '#VIA DISBATCHER#'] + args
            self.db_thread = Thread(target=main, name='disBatch driver', args=(kvsq,))
            self.db_thread.start()
            kvsserver = kvsq.get()
            sys.argv = save_sa
        else:
            self.db_thread = None

        self.donetask = tasksname + ' done!'
        self.resultkey = tasksname + ' result'
        self.taskkey = tasksname + ' task'

        self.kvs = kvsstcp.KVSClient(kvsserver)
        self.kvs.put('task source name', tasksname, False)
        self.kvs.put('task source done task', self.donetask, False)
        self.kvs.put('task source result key', self.resultkey, False)
        self.kvs.put('task source task key', self.taskkey, False)

        self.tid2status = {}

    def done(self):
        """Tell disBatch that there are no more tasks, which will cause it to
        shutdown when existing tasks have completed."""
        self.kvs.put(self.taskkey, self.donetask, False)
        if self.db_thread:
            self.db_thread.join()

    def submit(self, c):
        """Add a task to the disBatch queue. These can include #DISBATCH
        directives. It is up to the user to track the corresponding task
        ids."""
        self.kvs.put(self.taskkey, c, False)

    def syncTasks(self, taskIds):
        """Wait for specified task ids to complete and collect results,
        returning a dictionary from task id to status report, itself a
        dictionary in json format."""
        tid2status = {}
        for tid in taskIds:
            # Gather results until we find the one we are looking for.
            while tid not in self.tid2status:
                self.wait_one_task()
            tid2status[tid] = self.tid2status[tid]
        return tid2status

    def wait_one_task(self):
        """Wait for a task (any task) to complete."""
        some_tid = int(self.kvs.get(f'{self.resultkey} done tasks', False))
        status = json.loads(
            self.kvs.get(f'{self.resultkey} {some_tid}', False).decode('utf-8')
        )  # If encoding is False, we just get raw utf-8 bytes.
        self.tid2status[some_tid] = status
        return status


class DisBatchInfo:
    def __init__(self, args, name, uniqueId, wd):
        self.args, self.name, self.uniqueId, self.wd = args, name, uniqueId, wd


class BatchContext:
    USERWARNING = logging.WARNING + 1

    def __init__(self, sysid, dbInfo, rank, nodes, cylinders, cores_per_cylinder, args, contextLabel=None):
        if contextLabel is None:
            contextLabel = f'context{rank:05d}'
        self.sysid = sysid
        self.dbInfo = dbInfo
        self.rank = rank
        self.nodes = nodes
        self.cylinders = cylinders
        self.cores_per_cylinder = cores_per_cylinder
        self.args = args
        self.label = contextLabel

        self.error = False  # engine errors (non-zero return values)
        self.kvsKey = f'.context_{rank:d}'
        self.retireCmd = None

    def __str__(self):
        return f'Context type: {self.sysid}\nLabel: {self.label}\nNodes: {self.nodes!r}\nCylinders: {self.cylinders!r}\nCores per cylinder: {self.cores_per_cylinder!r}\n'

    def finish(self):
        """Check that all engines completed successfully and return True on success."""
        for n, e in self.engines.items():
            r = killPatiently(e, 'engine ' + n)
            if r:
                self.error = True  # also handled by retireNode
        return not self.error

    def launch(self, kvs):
        """Launch the engine processes on all the nodes by calling launchNode for each."""
        kvs.put(self.kvsKey, self)
        kvs.put('.controller', ('context info', self))
        self.engines = dict()  # live subprocesses
        for x, n in enumerate(self.nodes):
            if self.cylinders[x]:
                self.engines[n] = self.launchNode(n)
            else:
                logging.info(f'Skipping launch for {n}, no cylinders available.')

    def launchNode(self, node):
        """Launch an engine for a single node.  Should return a subprocess handle (unless launch itself is overridden)."""
        raise NotImplementedError(f'{type(self)}.launchNode is not implemented')

    def poll(self):
        """Check if any engines have stopped."""
        for n, e in list(self.engines.items()):
            r = e.poll()
            if r is not None:
                logger.info('Engine %s exited: %d', n, r)
                del self.engines[n]
                self.retireNode(n, r)

    def retireEnv(self, nodeList, retList):
        """Generate an environment for running the retirement command for a given list of nodes."""
        env = os.environ.copy()
        env['NODES'] = ','.join(nodeList)
        env['RETCODES'] = ','.join([str(rc) for rc in retList])
        env['ACTIVE'] = ','.join(self.engines.keys())
        env['RETIRED'] = ','.join(set(self.nodes).difference(self.engines))
        return env

    def retireNodeList(self, nodeList, retList):
        """Called when one or more nodes has exited.  May be overridden to release resources."""
        for ret in retList:
            if ret:
                self.error = True
                break

        if self.retireCmd:
            env = self.retireEnv(nodeList, retList)
            logger.info(
                'Retiring "%s" with command %s, env %s',
                nodeList,
                str(self.retireCmd),
                [(v, env.get(v, '<NOT SET>')) for v in ['DRIVER_NODE', 'NODES', 'RETCODES', 'ACTIVE', 'RETIRED']],
            )
            try:
                capture = SUB.run(self.retireCmd, close_fds=True, shell=True, env=env, check=True, capture_output=True)
            except Exception as e:
                logger.warning('Retirement planning needs improvement: %s', repr(e))
                capture = e
            finally:
                if capture.stdout:
                    logger.info('Retirement stdout: "%s"', capture.stdout.decode('utf-8'))
                if capture.stderr:
                    logger.info('Retirement stderr: "%s"', capture.stderr.decode('utf-8'))
        else:
            logger.info('Retiring "%s" (no-op)', nodeList)

    def retireNode(self, node, ret):
        self.retireNodeList([node], [ret])

    def run_task(self, target_rank, task_info, env):
        # Default implementation. May be overriden to implement a context-specific method.
        return SUB.Popen(
            ['/bin/bash', '-c', task_info.taskCmd],
            env=env,
            stdin=None,
            stdout=SUB.PIPE,
            stderr=SUB.PIPE,
            preexec_fn=os.setsid,
            close_fds=True,
        )

    def poll_task(self, p):
        # Default implementation. May be overriden to implement a context-specific method.
        return p.poll()

    def setNode(self, node=None):
        """Try to determine the hostname of this engine from the pov of the launcher."""
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
            raise LookupError(f'Couldn\'t find nodeId for {node or myHostname} in "{self.nodes}".')


# Convert nodelist format (slurm specific?) to an expanded list of nodes.
#    nl     => hosts[,nl]
#    hosts  => prefix[\[ranges\]]
#    ranges => range[,ranges]
#    range  => lo[-hi]
# where lo and hi are numbers
def nl2flat(nl):
    return SUB.check_output(['scontrol', 'show', 'hostnames', nl], universal_newlines=True).splitlines()


class SlurmContext(BatchContext):
    # Example Slurm environment
    # $ salloc -c 5 -n 10 --ntasks-per-node=3 -p scc -t 0-1
    # SLURM_CPUS_PER_TASK=5                                    ;; set if -c is specified
    # SLURM_TASKS_PER_NODE=3(x4)
    # SLURM_JOB_RESERVATION=rocky8
    # SLURM_NNODES=4
    # SLURM_NTASKS_PER_NODE=3                                  ;; set if --ntasks-per-node is specified
    # SLURM_JOB_NODELIST=worker[2080,5071-5072,5422]
    # SLURM_CLUSTER_NAME=slurm
    # SLURM_NODELIST=worker[2080,5071-5072,5422]
    # SLURM_NTASKS=12
    # SLURM_JOB_CPUS_PER_NODE=40,128(x3)
    # SLURM_JOB_NAME=interactive
    # SLURM_JOBID=1568029
    # SLURM_JOB_NUM_NODES=4
    # SLURM_NPROCS=12
    # SLURM_JOB_ID=1568029

    ThrottleTime = (
        10  # To avoid spamming Slurm, send retirement requests at a minimum interval of ThrottleTime seconds.
    )

    # Cannot pickle the throttle stuff.
    def __getstate__(self):
        state = self.__dict__.copy()
        del state['throttleq']
        del state['throttle_thread']
        return state

    def __init__(self, dbInfo, rank, args):
        # Note: the context is used to set the log file name, so
        # logging isn't available now. Keep a list of log messages to
        # report later.
        self.for_log = []

        jobid = os.environ['SLURM_JOB_ID']
        nodes = nl2flat(os.environ['SLURM_NODELIST'])

        def decodeSlurmVal(val):
            vv = []
            for e in val.split(','):
                m = re.match(r'([^\(]+)(?:\(x([^\)]+)\))?', e)
                c, m = m.groups()
                if m is None:
                    m = '1'
                vv += [int(c)] * int(m)
            return vv

        cores_per_cylinder = None

        scpt = os.environ.get('SLURM_CPUS_PER_TASK')
        if args.cpusPerTask != -1.0:
            self.cpusPerTask = args.cpusPerTask
            cores_per_cylinder = [int(args.cpusPerTask)] * len(nodes)
            if scpt:
                if self.cpusPerTask != int(scpt):
                    self.for_log.append(
                        (
                            f'disBatch argument cpusPerTask ({self.cpusPerTask}) conflicts with SLURM_CPUS_PER_TASK ({scpt}). Using disBatch value.',
                            self.USERWARNING,
                        )
                    )
        else:
            self.cpusPerTask = int(scpt) if scpt else 1

        sntpn = os.environ.get('SLURM_NTASKS_PER_NODE')
        self.tasksPerNode = None
        if args.tasksPerNode != -1:
            self.tasksPerNode = args.tasksPerNode
            if sntpn:
                self.for_log.append(
                    (
                        f'Argument tasksPerNode is set to {self.tasksPerNode}, ignoring SLURM_NTASKS_PER_NODE ({sntpn})',
                        logging.INFO,
                    )
                )
                if self.tasksPerNode != int(sntpn):
                    self.for_log.append(
                        (
                            f'disBatch argument tasksPerNode ({self.tasksPerNode}) conflicts with SLURM_NTASKS_PER_NODE ({sntpn}). Using disBatch value.',
                            self.USERWARNING,
                        )
                    )
        elif sntpn:
            self.tasksPerNode = int(sntpn)

        jcpnl = decodeSlurmVal(os.environ['SLURM_JOB_CPUS_PER_NODE'])
        self.stpnl = decodeSlurmVal(os.environ['SLURM_TASKS_PER_NODE'])

        if self.tasksPerNode:
            cylinders = [self.tasksPerNode] * len(nodes)
        else:
            do_fill = False
            if args.fill:
                if 'CUDA_VISIBLE_DEVICES' in os.environ or 'GPU_DEVICE_ORDINAL' in os.environ:
                    self.for_log.append(('Fill requested, but GPUs detected. Ignoring request.', logging.WARNING))
                else:
                    do_fill = True
            if do_fill:
                self.for_log.append(('Fill requested, ignoring SLURM_TASKS_PER_NODE.', logging.WARNING))
                cylinders = [int(jcpn // self.cpusPerTask) for jcpn in jcpnl]
                self.for_log.append(
                    (f'Cores per node: {jcpnl} /({self.cpusPerTask} cores per task) -> {cylinders}', logging.INFO)
                )
            else:
                # Follow SLURM_TASKS_PER_NODE, but honor cpusPerTask
                cylinders = [min(stpn, int(jcpn // self.cpusPerTask)) for stpn, jcpn in zip(self.stpnl, jcpnl)]
                self.for_log.append(
                    (
                        f'Tasks per node: {self.stpnl} -> {cylinders}, using {self.cpusPerTask} cores per task.',
                        logging.INFO,
                    )
                )
        if cores_per_cylinder is None:
            cores_per_cylinder = [jcpn // c if c else jcpn for jcpn, c in zip(jcpnl, cylinders)]

        # Provide a hook to allow the user to alter srun options.
        opt_file = os.environ.get('DISBATCH_SLURM_SRUN_OPTIONS_FILE', None)
        opts = []
        if opt_file:
            self.for_log.append((f'Taking srun options from "{opt_file}".', logging.INFO))
            opts = open(opt_file).read().split('\n')
        else:
            opts = ['SLURM_CPU_BIND=' + os.getenv('SLURM_CPU_BIND', 'cores'), 'SLURM_MPI_TYPE=none']
        if opts:
            self.for_log.append(('Adding srun options:', logging.INFO))
            for L in opts:
                if L:
                    self.for_log.append(('    ' + L, logging.INFO))
                    name, value = L.split('=', 1)
                    os.environ[name] = value

        contextLabel = args.label if args.label else f'J{jobid}'
        super().__init__('Slurm', dbInfo, rank, nodes, cylinders, cores_per_cylinder, args, contextLabel)
        self.driverNode = None
        self.retireCmd = 'scontrol update JobId="$SLURM_JOBID" NodeList="${DRIVER_NODE:+$DRIVER_NODE,}$ACTIVE"'
        self.throttleq = Queue()
        self.throttle_thread = Thread(target=self.__retirementThrottle__, name='throttle', daemon=True)
        self.throttle_thread.start()

    def engine_start(tag):
        # For a Slurm context, we use srun to start up independent
        # task servers. This ensures they get the resources Slurm
        # intends each task to have. Each engine cylinder has an
        # associated task server that executes tasks on its behalf
        # using the resources assigned by Slurm.

        # Each cylinder communicates with its task server using a
        # collection of named pipes to send tasks to and get per-task
        # stdout, stderr pipe names and returns codes from the server.
        # The cylinder runs a simple proxy that uses the stdout and
        # stderr pipes to provide appropriate connection points for
        # Popen stdout and stderr.

        to_server_template, from_server_template = f'/tmp/{tag}_to_task_server_%d', f'/tmp/{tag}_from_task_server_%d'

        slurm_local_rank = int(os.environ['SLURM_LOCALID'])
        ranks = int(os.environ['SLURM_NTASKS'])
        if slurm_local_rank == 0:
            # Fork and return control to what will become the engine.
            if os.fork() > 0:
                incoming_pipes, outgoing_pipes = [], []
                for r in range(ranks):
                    incoming_pipes.append(waitForIt(from_server_template % r, 'rb'))
                    outgoing_pipes.append(open(to_server_template % r, 'wb'))
                # This code runs at start up, before there is an
                # context object. Return this state as an opaque
                # object, that will be used to update the context
                # object when we finnaly have one.
                return (incoming_pipes, outgoing_pipes)

        to_server, from_server = to_server_template % slurm_local_rank, from_server_template % slurm_local_rank

        os.mkfifo(to_server, 0o600)
        os.mkfifo(from_server, 0o600)

        # Ordering matters here.
        outgoing = open(from_server, 'wb')
        incoming = waitForIt(to_server, 'rb')
        os.unlink(from_server)
        os.unlink(to_server)

        while True:
            # Receive task info from the cylinder.
            (seq, cmd, env) = pickle.load(incoming)
            if seq is None:
                break

            # Set up stdout and stderr pipes for this task. The cylinder will delete these pipes.
            sh_err, sh_out = f'/tmp/{tag}_{slurm_local_rank}_{seq}_err', f'/tmp/{tag}_{slurm_local_rank}_{seq}_out'
            os.mkfifo(sh_err, 0o600)
            os.mkfifo(sh_out, 0o600)
            # Send the pipe names back to the cylinder.
            pickle.dump((sh_err, sh_out), outgoing)
            outgoing.flush()
            p = SUB.Popen(
                ['/bin/bash', '-c', cmd], env=env, stdin=None, stdout=open(sh_out, 'wb'), stderr=open(sh_err, 'wb')
            )
            r = p.wait()
            # Send the pid and return code.
            pickle.dump((p.pid, r), outgoing)
            outgoing.flush()

        incoming.close()
        outgoing.close()
        sys.exit(0)

    def engine_state(self, opaque):
        # Process state that was created by engine_start before we had
        # a context object. Note this name must be the name of the
        # engine start method + '_return'
        self.incoming_pipes, self.outgoing_pipes = opaque

    def engine_stop(self):
        # Process state that was created by engine_start before we had
        # a context object. Note this name must be the name of the
        # engine start method + '_return'
        for p in self.outgoing_pipes:
            pickle.dump((None, None, None), p)
            p.flush()

    def launchNode(self, n):
        lfp = f'{self.dbInfo.uniqueId}_{self.label}_{n}_engine_wrap.log'
        # To convince Slurm to give us the right gres, request the right number of tasks.
        nx = self.nodes.index(n)
        tasks = self.cylinders[nx]
        if self.stpnl[nx] != tasks:
            logging.warning(f'Slurm believes tasks should be {self.stpnl[nx]}, attempting to run {tasks}.')
        # srun the appropriate number of task servers for this node. 0-rank will fork off the engine proper.
        # The logic for this is in SlurmContext.engine_start.
        # SLURM_CPU_BIND is set in the environment, since the user might want to override that
        cmd = [
            'srun',
            '-N',
            '1',
            '-n',
            str(tasks),
            '-c',
            str(self.cores_per_cylinder[nx]),
            '-w',
            n,
            'bash',
            '-c',
            f'{DbUtilPath} --engine -n {n} --method SlurmContext.engine {self.kvsKey} --tag slurm_context_engine_{int(10e7*random.random())}',
        ]
        logging.info('launch cmd: %s', repr(cmd))
        return SUB.Popen(cmd, stdout=open(lfp, 'w'), stderr=SUB.STDOUT, close_fds=True)

    def __retirementThrottle__(self):
        nodeList, retList = [], []
        while True:
            try:
                node, ret = self.throttleq.get(timeout=self.ThrottleTime)
                logging.info(f'Throttle: {node}, {ret}.')
                nodeList.append(node)
                retList.append(ret)
                self.throttleq.task_done()
            except Empty:
                if nodeList:  # Since the queue signaled empty, it has
                    # been at least ThrottleTime since the
                    # last node was added.
                    logging.info(f'Throttle releasing: {nodeList}, {retList}.')
                    super().retireNodeList(nodeList, retList)
                    nodeList, retList = [], []

    def retireEnv(self, nodeList, retList):
        env = super().retireEnv(nodeList, retList)
        if self.driverNode:
            env['DRIVER_NODE'] = self.driverNode
        return env

    def retireNode(self, node, ret):
        logging.info(f'Retiring {node} ({ret}).')
        # If this is the node on which the context manager is running, driverNode will be set.
        # The driverNode is never retired; it is always included in scontrol update "NodeList=...".
        if compHostnames(node, myHostname):
            self.driverNode = node
        self.throttleq.put((node, ret))
        self.throttleq.join()  # For thread safety, wait for an ack.
        logging.info('Throttle has acked.')

    def run_task(self, target_cylinder, task_info, env):
        incoming, outgoing = self.incoming_pipes[target_cylinder], self.outgoing_pipes[target_cylinder]
        pickle.dump((task_info.taskId, task_info.taskCmd, env), outgoing)
        outgoing.flush()
        sh_err, sh_out = pickle.load(incoming)
        p = SUB.Popen(
            ['/bin/bash', '-c', f'cat < {sh_out} & cat < {sh_err} >&2'], stdin=None, stdout=SUB.PIPE, stderr=SUB.PIPE
        )
        # Avoid the temptation to neaten things up by unlinking the
        # out and err pipes now---process start up is asynchronous so
        # there could be a race.

        # TODO: How evil is this? Should we create a subclass of Popen objects?
        p.slurm_context_incoming, p.slurm_context_outoging = incoming, outgoing
        p.sh_out, p.sh_err = sh_out, sh_err
        return p

    def poll_task(self, p):
        r = p.poll()
        if r is not None:
            remote_pid, remote_rc = pickle.load(p.slurm_context_incoming)
            os.unlink(p.sh_err)
            os.unlink(p.sh_out)
            p.pid = remote_pid
            return remote_rc
        else:
            return None

    def setNode(self, node=None):
        super().setNode(node or os.getenv('SLURMD_NODENAME'))


# TODO:
# class GEContext(BatchContext):
# class LSFContext(BatchContext):
# class PBSContext(BatchContext):


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
        # Record messages for logging once the logger is started.
        self.for_log = []

        nodelist = args.ssh_node if args.ssh_node else os.getenv('DISBATCH_SSH_NODELIST')
        contextLabel = args.label if args.label else f'SSH{rank:d}'

        core_count, node_set, nodes = [], set(), []
        if type(nodelist) is not str:
            nodelist = ','.join(nodelist)
        for p in nodelist.split(','):
            p = p.strip()
            if not p:
                continue
            try:
                n, e = p.rsplit(':', 1)
                e = int(e)
            except ValueError:
                raise ValueError('SSH nodelist items must be HOST:CORECOUNT')

            if n == 'localhost':
                n = myHostname
            if n in node_set:
                nx = nodes.index(n)
                self.for_log.append((f'Repeated {n} so summing cores {core_count[nx]} + {e}', logging.WARN))
                core_count[nx] += e
            else:
                node_set.add(n)
                nodes.append(n)
                core_count.append(e)
        self.for_log.append((f'nodes: {nodes}, cores: {core_count}', logging.INFO))

        if args.cpusPerTask != -1.0:
            self.cpusPerTask = args.cpusPerTask
        else:
            self.cpusPerTask = 1

        if args.tasksPerNode != -1:
            self.tasksPerNode = args.tasksPerNode
        else:
            self.tasksPerNode = None

        if self.tasksPerNode:
            cylinders = [self.tasksPerNode] * len(nodes)
        else:
            cylinders = [int(cc // self.cpusPerTask) for cc in core_count]
            self.for_log.append(
                (f'Tasks per node: {cylinders}, using {self.cpusPerTask} cores per task.', logging.INFO)
            )
        cores_per_cylinder = [cc // c if c else cc for cc, c in zip(core_count, cylinders)]
        super().__init__('SSH', dbInfo, rank, nodes, cylinders, cores_per_cylinder, args, contextLabel)

    def launchNode(self, n):
        prefix = [] if compHostnames(n, myHostname) else ['ssh', n]
        lfp = f'{self.dbInfo.uniqueId}_{self.label}_{n}_engine_wrap.log'
        cmd = prefix + [DbUtilPath, '--engine', '-n', n, self.kvsKey]
        logger.info('ssh launch comand: %r', cmd)
        return SUB.Popen(cmd, stdin=open(os.devnull), stdout=open(lfp, 'w'), stderr=SUB.STDOUT, close_fds=True)


def probeContext(dbInfo, rank, args):
    if 'SLURM_JOBID' in os.environ:
        return SlurmContext(dbInfo, rank, args)
    # if ...: return GEContext()
    # if ...: LSFContext()
    # if ...: PBSContext()
    if 'DISBATCH_SSH_NODELIST' in os.environ:
        return SSHContext(dbInfo, rank, args)


class TaskInfo:
    kinds = {
        'B': 'barrier',
        'C': 'check barrier',
        'D': 'done',
        'N': 'normal',
        'P': 'per node',
        'S': 'skip',
        'Z': 'zombie',
    }

    def __init__(self, taskId, taskStreamIndex, taskRepIndex, taskCmd, taskKey, kind='N', bKey=None, skipInfo=None):
        self.taskId, self.taskStreamIndex, self.taskRepIndex, self.taskCmd, self.taskKey, self.bKey = (
            taskId,
            taskStreamIndex,
            taskRepIndex,
            taskCmd,
            taskKey,
            bKey,
        )
        assert kind in TaskInfo.kinds
        self.kind = kind
        assert skipInfo is None or self.kind == 'S'
        self.skipInfo = skipInfo

    def __eq__(self, other):
        if type(self) is not type(other):
            return False
        sti, oti = self, other
        return (
            sti.taskId == oti.taskId
            and sti.taskStreamIndex == oti.taskStreamIndex
            and sti.taskRepIndex == oti.taskRepIndex
            and sti.taskCmd == oti.taskCmd
        )  # and sti.taskKey == oti.taskKey

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return '\t'.join(
            [
                str(x)
                for x in [
                    self.taskId,
                    self.taskStreamIndex,
                    self.taskRepIndex,
                    self.kind,
                    self.taskCmd.decode('utf-8', 'replace'),
                ]
            ]
        )


class TaskReport:
    header = 'ROE[ PSZ]\tTaskID\tLineNum\tRepeatIndex\tNode\tPID\tReturnCode\tElapsed\tStart\tFinish\tBytesOfLeakedOutput\tOutputSnippet\tBytesOfLeakedError\tErrorSnippet\tCommand'
    fields = [
        'Flags',
        'TaskId',
        'TaskStreamIndex',
        'TaskRepIndex',
        'Host',
        'PID',
        'ReturnCode',
        'Elapsed',
        'Start',
        'End',
        'OutBytes',
        'OutData',
        'ErrBytes',
        'ErrData',
        'TaskCmd',
    ]
    # For the moment, parsing a task report boils down to splitting on
    # a fixed number of tabs. Keep the task command last to avoid
    # issues with embedded tabs. This makes for cleaner display too.
    assert fields[-1] == 'TaskCmd'

    field2index = dict([(f, x) for x, f in enumerate(fields)])
    num_fields = len(fields)

    @staticmethod
    def find_field(ff, f):
        return ff[TaskReport.field2index[f]]

    def __init__(self, *args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0 and isinstance(args[0], str):
            # In effect this undoes __str__, so this must be kept in sync with __str__.
            try:
                if args[0] == TaskReport.header:
                    self.taskInfo = None
                else:
                    fx = partial(TaskReport.find_field, args[0].split('\t', TaskReport.num_fields - 1))
                    kind = 'N'
                    flags = fx('Flags')
                    if flags[3] in 'PSZ':
                        kind = flags[3]
                    elif flags[0] not in ' R':
                        kind = flags[0]
                    tc = fx('TaskCmd')
                    if tc.startswith("b'"):
                        tc = literal_eval(tc)
                    else:
                        tc = tc.encode('utf-8')
                    ti = TaskInfo(
                        int(fx('TaskId')), int(fx('TaskStreamIndex')), int(fx('TaskRepIndex')), tc, '', kind=kind
                    )
                    self.do_init(
                        ti,
                        fx('Host'),
                        int(fx('PID')),
                        int(fx('ReturnCode')),
                        float(fx('Start')),
                        float(fx('End')),
                        int(fx('OutBytes')),
                        literal_eval(fx('OutData')),
                        int(fx('ErrBytes')),
                        literal_eval(fx('ErrData')),
                    )
            except Exception:
                logger.exception('Task reporting')
                self.taskInfo = None
        else:
            self.do_init(*args, **kwargs)

    def do_init(
        self,
        taskInfo,
        host=myHostname,
        pid=myPid,
        returncode=0,
        start=0,
        end=0,
        outbytes=0,
        outdata='',
        errbytes=0,
        errdata='',
    ):
        self.taskInfo = taskInfo
        self.host = host
        self.pid = pid
        self.returncode = returncode
        self.start = start
        self.end = end
        self.outbytes = outbytes
        self.outdata = outdata
        self.errbytes = errbytes
        self.errdata = errdata
        self.engineReport = None  # This will be filled in by EngineBlock.

    def flags(self):
        if self.taskInfo.kind in 'NPSZ':
            return (
                ('R' if self.returncode else ' ')
                + ('O' if self.outbytes else ' ')
                + ('E' if self.errbytes else ' ')
                + (self.taskInfo.kind if self.taskInfo.kind in 'PSZ' else ' ')
            )
        else:
            return self.taskInfo.kind + '    '

    def reportDict(self):
        ti = self.taskInfo
        rd = dict(
            zip(
                TaskReport.fields,
                [
                    self.flags(),
                    ti.taskId,
                    ti.taskStreamIndex,
                    ti.taskRepIndex,
                    self.host,
                    self.pid,
                    self.returncode,
                    (self.end - self.start),
                    self.start,
                    self.end,
                    self.outbytes,
                    self.outdata,
                    self.errbytes,
                    self.errdata,
                    ti.taskCmd,
                ],
            )
        )
        return rd

    def __str__(self):
        rd = self.reportDict()
        for format, fn in [
            ('%.3f', 'Elapsed'),
            ('%.3f', 'Start'),
            ('%.3f', 'End'),
            ('%r', 'OutData'),
            ('%r', 'ErrData'),
        ]:
            rd[fn] = format % rd[fn]
        try:
            # The status file is UTF-8. If we can interpret the
            # TaskCmd bytes as a UTF-8 string, do that, otherwise
            # report the bytes str() representation.
            rd['TaskCmd'] = rd['TaskCmd'].decode('utf-8')
        except UnicodeDecodeError:
            pass
        return '\t'.join([str(rd[f]) for f in TaskReport.fields])


def parseStatusFiles(*files):
    status = dict()
    for f in files:
        try:
            with open(f, encoding='utf-8') as s:
                for L in s:
                    tr = TaskReport(L[:-1])
                    ti = tr.taskInfo
                    if ti is None:
                        continue
                    if ti.kind != 'N':
                        continue
                    ti.kind, ti.skipInfo = 'S', tr  # This creates a reference loop!
                    try:
                        # successful tasks take precedence
                        if status[ti.taskId].skipInfo.returncode <= tr.returncode:
                            continue
                    except KeyError:
                        pass
                    status[ti.taskId] = ti
        except Exception:
            logger.exception('Parsing status file.')
            return None
    return status


##################################################################### DRIVER


# When the user specifies tasks will be passed through a KVS, this
# class generates an interable that feeds task from the KVS.
class KVSTaskSource:
    def __init__(self, kvs):
        self.kvs = kvs.clone()

    def waitForSignIn(self):
        # These are handled separately, because we need to accommodate
        # clients written in languages other than python.
        self.name = self.kvs.get('task source name', False)
        self.donetask = self.kvs.get('task source done task', False)
        self.resultkey = self.kvs.get('task source result key', False)
        self.taskkey = self.kvs.get('task source task key', False)

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
class TaskProcess:
    def __init__(self, taskSource, command, **kwargs):
        self.taskSource = taskSource
        self.command = command
        self.p = SUB.Popen(command, **kwargs)
        self.r = None

    def poll(self):
        if self.r is not None:
            return
        self.r = self.p.poll()
        if self.r is None:
            return
        # TODO: send done on success
        logger.info('Task generating command has exited: %s %d.', repr(self.command), self.r)
        # post a done just in case the process didn't
        self.taskSource.done()


class TaskGenerator:
    """
    Given a task source (generating task command lines), parse the lines and
    yield TaskInfos.

    The generator also knows when it has no more user tasks, as indicated by
    the `done` property.

    A filter is also accepted, since we can't wrap this generator. Wrapping it
    would change its type and hide the `done` property.
    """

    def __init__(self, tasks, filter=None):
        self._tasks = tasks
        self._done = False
        self._generator = self._task_generator()
        if filter:
            self._generator = filter(self._generator)

        # We look ahead by one task because we need to actually
        # start constructing tasks to know if we are done.
        logger.info('Fetching first task, could block')
        self._buffer_next()

    def __iter__(self):
        return self

    def __next__(self):
        if self._exception:
            raise self._exception

        val = self._val
        self._buffer_next()
        return val

    def _buffer_next(self):
        try:
            self._val = next(self._generator)
            self._exception = None
        except StopIteration as e:
            self._exception = e
            self._val = None

    @property
    def done(self):
        return self._done

    def _task_generator(self):
        tsx = 0  # "line number" of current task
        taskCounter = 0  # next taskId
        peCounters = {'START': 0, 'STOP': 0}
        perEngineAllowed = True
        prefix = suffix = b''

        def peEndListTasks():
            for when in ['START', 'STOP']:
                yield TaskInfo(
                    peCounters[when], tsx, -1, b'#ENDLIST', f'.per engine {when:s} {peCounters[when]:d}', kind='P'
                )

        OK = True
        while OK:
            tsx += 1
            try:
                t = next(self._tasks)
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
                if not t.startswith(b'#DISBATCH') and dbcomment.match(t):
                    # Comment or empty line, ignore
                    continue

                m = dbperengine.match(t)
                if m:
                    if not perEngineAllowed:
                        # One could imagine doing some sort of per-engine
                        # reset with each barrier, but that would get
                        # messy pretty quickly.
                        logger.error('Per-engine tasks not permitted after normal tasks.')
                        OK = False
                        break
                    when, cmd = m.groups()
                    when = when.decode('ascii')
                    cmd = prefix + cmd + suffix
                    yield TaskInfo(
                        peCounters[when], tsx, -1, cmd, f'.per engine {when:s} {peCounters[when]:d}', kind='P'
                    )
                    peCounters[when] += 1
                    continue

                m = dbprefix.match(t)
                if m:
                    prefix = m.group(1)
                    continue

                m = dbsuffix.match(t)
                if m:
                    suffix = m.group(1)
                    continue

                if perEngineAllowed:
                    # Close out the per-engine task block.
                    perEngineAllowed = False
                    yield from peEndListTasks()

                m = dbrepeat.match(t)
                if m:
                    repeats, rx, step = int(m.group('repeat')), 0, 1
                    g = m.group('start')
                    if g:
                        rx = int(g)
                    g = m.group('step')
                    if g:
                        step = int(g)
                    logger.info('Processing repeat: %d %d %d', repeats, rx, step)
                    cmd = prefix + (m.group('command') or b'') + suffix
                    while repeats > 0:
                        yield TaskInfo(taskCounter, tsx, rx, cmd, '.task')
                        taskCounter += 1
                        rx += step
                        repeats -= 1
                    continue

                m = dbbarrier.match(t)
                if m:
                    check, bKey = m.groups()
                    kind = 'C' if check else 'B'
                    yield TaskInfo(taskCounter, tsx, -1, t, '.barrier', kind=kind, bKey=bKey)
                    taskCounter += 1
                    continue

                if t.startswith(b'#DISBATCH '):
                    logger.error('Unknown #DISBATCH directive: %s', t)
                else:
                    yield TaskInfo(taskCounter, tsx, -1, prefix + t + suffix, '.task')
                    taskCounter += 1

        self._done = True
        if perEngineAllowed:
            # Handle edge case of no tasks.
            yield from peEndListTasks()

        logger.info('Processed %d tasks.', taskCounter)


def statusTaskFilter(tasks, status, retry=False, force=False):
    if status is None:  # This means an error occurred while reading status files.
        return
    while True:
        try:
            t = next(tasks)
        except StopIteration:
            break
        if t.kind == 'N':
            s = status.get(t.taskId)
            if s and (not retry or s.skipInfo.returncode == 0):
                # skip
                if s != t:
                    msg = 'Recovery status file task mismatch %s:\n' + repr(s) + '\n' + repr(t)
                    if force:
                        logger.warning(msg, '-- proceeding anyway')
                    else:
                        raise Exception(msg % '(use --force-resume to proceed anyway)')
                yield s
                continue
        yield t


# Main control loop that sends new tasks to the execution engines.
class Feeder(Thread):
    def __init__(self, kvs, ageQ, tasks, slots):
        super().__init__(name='Feeder')
        self.kvs = kvs.clone()
        self.ageQ = ageQ
        self.taskGenerator = tasks
        self.slots = slots

        self.daemon = True
        self.start()

    def run(self):
        try:
            self.main()
        except Exception:
            logger.exception('Feeder')
            self.kvs.put('.controller', ('feeder exception', None))
            raise

    def main(self):
        lastId = -1
        while True:
            try:
                tinfo = next(self.taskGenerator)
            except StopIteration:
                self.kvs.put('.controller', ('no more tasks', lastId + 1))
                break

            if tinfo.kind in 'BC':
                self.kvs.put('.controller', ('special task', tinfo))
                # Synchronization control.
                logger.debug('Feeder waiting for %d.', tinfo.taskId)
                t = self.ageQ.get()
                if t == 'CheckFailExit':
                    logger.info('Feeder told to exit.')
                    break
                assert t == tinfo.taskId
                continue

            if tinfo.kind in 'NS':
                lastId = tinfo.taskId

            if tinfo.kind == 'N':
                # Flow control. Wait for an available cylinder.
                self.slots.get()

            logger.debug('Feeding task: %s ', tinfo)
            self.kvs.put('.controller', ('task', tinfo))

        self.kvs.close()


# Main control loop that processes completed tasks.
class Driver(Thread):
    def __init__(self, kvs, db_info, tasks, trackResults=None):
        super().__init__(name='Driver')
        self.kvs = kvs.clone()
        self.db_info = db_info
        # uniqueId can have a path component. Remove that here.
        self.kvs.put(
            '.common env',
            {'DISBATCH_JOBID': os.path.split(str(self.db_info.uniqueId))[-1], 'DISBATCH_NAMETASKS': self.db_info.name},
        )
        self.trackResults = trackResults

        self.ageQ = Queue()
        self.slots = Queue()
        self.feeder = Feeder(self.kvs, self.ageQ, tasks, self.slots)

        self.barriers, self.barrierCount = [], 0
        self.contextCount = 0
        self.contexts = {}
        self.currentReturnCode = 0
        self.engineCount = 0
        self.engines = {}
        self.failed = 0
        self.finished = 0
        self.statusFile = open(db_info.uniqueId + '_status.txt', 'w+', encoding='utf-8')
        if db_info.args.status_header:
            print(TaskReport.header, file=self.statusFile)
        self.statusLastOffset = self.statusFile.tell()
        self.noMoreTasks = False
        self.tasksDone = False
        self.failFast = db_info.args.fail_fast

        self.daemon = True
        self.start()

    def recordResult(self, tReport):
        self.statusFile.write(str(tReport) + '\n')
        self.statusFile.flush()

    def sendNotification(self):
        try:
            import smtplib
            from email.mime.text import MIMEText

            self.statusFile.seek(self.statusLastOffset)
            mailTo = self.db_info.args.mailTo
            msg = MIMEText(f'Last {self.db_info.args.mailFreq + self.statusFile.read():d}:\n\n')
            msg['Subject'] = f'{self.db_info.uniqueId:s} has completed {self.finished:d} tasks'
            if self.failed:
                msg['Subject'] += f' ({self.failed:d} failed)'
            msg['From'] = mailTo
            msg['To'] = mailTo
            s = smtplib.SMTP()
            s.connect()
            s.sendmail([mailTo], [mailTo], msg.as_string())
            self.statusLastOffset = self.statusFile.tell()
        except Exception as e:
            logger.warning('Failed to send notification message: "%s". Disabling.', e)
            self.mailTo = None
            # Be sure to seek back to EOF to append
            self.statusFile.seek(0, 2)

    def stopContext(self, cRank):
        for e in self.engines.values():
            if e.cRank == cRank:
                e.stop()

    def updateStatus(self, activeCylinders):
        status = dict(
            more='No more tasks.' if self.noMoreTasks else 'More tasks.',
            barriers=self.barrierCount,
            contexts=self.contexts,
            currentReturnCode=self.currentReturnCode,
            engines=self.engines,
            activeCylinders=activeCylinders,
        )
        # Make changes visible via KVS.
        logger.debug('Posting status: %r', status)
        self.kvs.get('DisBatch status', False)
        self.kvs.put(
            'DisBatch status',
            json.dumps(status, default=lambda o: dict([t for t in o.__dict__.items() if t[0] != 'kvs'])),
            b'JSON',
        )

    class EngineProxy:
        def __init__(self, rank, cRank, hostname, pid, start, kvs):
            self.rank, self.cRank, self.hostname, self.pid, self.kvs = rank, cRank, hostname, pid, kvs
            # No need to clone kvs, this isn't a thread.
            self.active, self.assigned, self.cylinders, self.failed, self.finished = 0, 0, {}, 0, 0
            self.start = start
            self.status = 'running'
            self.last = time.time()

        def __str__(self):
            return (
                f'Engine {self.rank:d}: Context {self.cRank:d}, Host {self.hostname:s}, PID {self.pid:d}, '
                f'Started at {self.start:.2f}, Last heard from {time.time() - self.last:.2f}, '
                f'Cylinders {len(self.cylinders):d}, Assigned {self.assigned:d}, Finished {self.finished:d}, '
                f'Failed {self.failed:d}'
            )

        def addCylinder(self, pid, pgid, ckey):
            self.active += 1
            self.cylinders[ckey] = (pid, pgid, ckey)

        def dropCylinder(self, ckey):
            self.active -= 1
            self.kvs.put(ckey, ('stop', None))

        def stop(self):
            if self.status == 'stopped':
                return
            logger.info('Stopping engine %s', self)
            for ckey in self.cylinders:
                self.dropCylinder(ckey)
            self.status = 'stopping'

        def stopped(self, status):
            self.status = 'stopped'
            self.last = time.time()
            logger.info('Engine %d stopped, %s', self.rank, status)

    def run(self):
        self.kvs.put('DisBatch status', '<Starting...>', False)

        cRank2taskCount, cylKey2eRank, enginesDone, finishedTasks, hbFails = DD(int), {}, False, {}, set()
        notifiedAllDone, outstanding, pending, retired = False, {}, [], -1
        assignedCylinders, freeCylinders = set(), set()
        while not (self.tasksDone and enginesDone):
            logger.debug(
                'Driver loop: Finished %d, Retired %d, Available %d, Assigned %d, Free %d, Pending %d',
                self.finished,
                retired,
                self.slots.qsize(),
                len(assignedCylinders),
                len(freeCylinders),
                len(pending),
            )

            # Wait for a message.
            msg, o = self.kvs.get('.controller')

            logger.debug('Incoming msg: %s %s', msg, o)
            if msg == 'clearing barriers':
                pass
            elif msg == 'context info':
                context = o
                self.contexts[context.rank] = context
            elif msg == 'cylinder available':
                engineRank, cpid, cpgid, ckey = o
                e = self.engines[engineRank]
                # The engine proxy needs to know about the cylinder,
                # even if we are just going to end up stopping it,
                # since stopping it require a handshake with the real
                # engine process.
                e.addCylinder(cpid, cpgid, ckey)
                if e.status != 'running' or notifiedAllDone:
                    logger.info('Engine %s (%s), "%s" ignored, %s', engineRank, e.hostname, ckey, e.status)
                    e.dropCylinder(ckey)
                else:
                    cylKey2eRank[ckey] = engineRank
                    logger.info('Engine %s (%s), "%s" available', engineRank, e.hostname, ckey)
                    freeCylinders.add(ckey)
                    self.slots.put(True)
            elif msg == 'cylinder stopped':
                engineRank, ckey = o
                logger.info('Engine %s, "%s" stopped', engineRank, ckey)
                try:
                    # Do we need to checked is this cylinder has a currently assigned task, i.e., can stopped overtake finished task?
                    freeCylinders.remove(ckey)
                except KeyError:
                    pass
            elif msg == 'driver heart beat':
                now = time.time()
                if self.tasksDone:
                    enginesDone = True
                    for e in self.engines.values():
                        if e.status != 'stopped':
                            if (now - e.last) > NoPulse:
                                logger.info('Heart beat failure for engine %s.', e)
                                e.status = 'heart beat failure'  # This doesn't mean much at the moment.
                            else:
                                logger.debug('Engine %d in ICU (%.1f)', e.rank, (now - e.last))
                                enginesDone = False
                else:
                    for tinfo, ckey, start, ts in outstanding.values():
                        if now - ts > NoPulse:
                            logger.info(
                                'Heart beat failure for engine %s, cylinder %s, task %s.',
                                self.engines[cylKey2eRank[ckey]],
                                ckey,
                                tinfo,
                            )
                            if tinfo.taskId not in hbFails:  # Guard against a pile up of heart beat msgs.
                                hbFails.add(tinfo.taskId)
                                self.kvs.put('.controller', ('task hb fail', (tinfo, ckey, start, ts)))
            elif msg == 'engine heart beats':
                now = time.time()
                engineRank, taskHeartBeats = o
                self.engines[engineRank].last = now
                for tEngineRank, taskId in taskHeartBeats:
                    assert engineRank == tEngineRank
                    if taskId != -1:
                        if taskId in outstanding:
                            outstanding[taskId][3] = now
            elif msg == 'engine started':
                # TODO: reject if no more tasks or in shutdown?
                rank, cRank, hn, pid, start = o
                self.engines[rank] = self.EngineProxy(rank, cRank, hn, pid, start, self.kvs)
                # In server mode, we can have tasks waiting but no
                # engines, in which case enginesDone may be
                # "True". Adding this engine changes that.
                enginesDone = False
            elif msg == 'engine stopped':
                status, rank = o
                self.engines[rank].stopped(status)
                enginesDone = not [e for e in self.engines.values() if e.status != 'stopped']
            elif msg == 'feeder exception':
                logger.info('Emergency shutdown')
                break
            elif msg == 'no more tasks':
                self.noMoreTasks = True
                logger.info('No more tasks: %d accepted', o)
                self.barriers.append(
                    TaskReport(TaskInfo(o, -1, -1, b'#NO MORE TASKS BARRIER', None, kind='D'), start=time.time())
                )
                # If no tasks were actually processed, we won't
                # notice we are now done until the next heart beat, so
                # send one now to speed things along.
                self.kvs.put('.controller', ('driver heart beat', None))
            elif msg == 'register':
                which, key = o
                if self.tasksDone:
                    self.kvs.put(key, -1)
                else:
                    if which == 'context':
                        self.kvs.put(key, self.contextCount)
                        self.contextCount += 1
                    elif which == 'engine':
                        self.kvs.put(key, self.engineCount)
                        self.engineCount += 1
                    else:
                        logger.error('Register? %s for %s', which, key)
            elif msg == 'special task':
                tinfo = o
                assert tinfo.kind in 'BC'
                logger.info('Finishing barrier %d.', tinfo.taskId)
                # TODO: Add assertion to verify ordering property?
                self.barrierCount += 1
                self.barriers.append(TaskReport(tinfo, start=time.time()))
            elif msg == 'stop context':
                cRank = o
                self.stopContext(cRank)
            elif msg == 'stop engine':
                rank = o
                self.engines[rank].stop()
            elif msg == 'task':
                tinfo = o
                if tinfo.kind == 'P':
                    logger.info('Posting per engine task "%s" %s', tinfo.taskKey, tinfo)
                    if tinfo.taskCmd == b'#ENDLIST':
                        self.kvs.put(tinfo.taskKey, ('stop', None))
                    else:
                        self.kvs.put(tinfo.taskKey, ('task', tinfo))
                elif tinfo.kind == 'S':
                    logger.info('Skipping %s', tinfo.skipInfo)
                    self.kvs.put('.controller', ('task skipped', tinfo))
                else:
                    pending.append(tinfo)
            elif msg == 'task done' or msg == 'task hb fail' or msg == 'task skipped':
                hbFail, skipped, zombie = False, False, False
                if msg == 'task skipped':
                    tinfo = o
                    assert tinfo.kind == 'S'
                    tReport = tinfo.skipInfo
                    tinfo.skipInfo = None  # break circular reference.
                    skipped = True
                elif msg == 'task hb fail':
                    tinfo, ckey, start, last = o
                    assert tinfo.kind == 'N'
                    engineRank = cylKey2eRank[ckey]
                    tReport = TaskReport(tinfo, self.engines[engineRank].hostname, -1, -100, start, last)
                    hbFail = True
                else:
                    tReport, engineRank, cid, ckey = o
                    assert isinstance(tReport, TaskReport)
                    tinfo = tReport.taskInfo
                    if tinfo.taskId in hbFails:
                        tinfo.kind = 'Z'
                        logger.info('Zombie task done: %s', tReport)
                        self.recordResult(tReport)
                        zombie = True

                if not zombie:
                    rc = tReport.returncode

                    assert tinfo.kind in 'NPS'
                    self.recordResult(tReport)

                    if tinfo.kind != 'P':
                        # Track non-per-engine task completion.
                        # This is used to implement barriers.
                        finishedTasks[tinfo.taskId] = True

                    if tinfo.kind == 'N':
                        outstanding.pop(tinfo.taskId)

                    # TODO: Count per engine?
                    self.finished += 1

                    if not skipped and not hbFail:
                        e = self.engines[engineRank]
                        e.last = time.time()
                        e.finished += 1
                        if tinfo.kind == 'N':
                            e.assigned -= 1
                            if e.status == 'running':
                                assignedCylinders.remove(ckey)
                                freeCylinders.add(ckey)
                                self.slots.put(True)
                    if rc:
                        self.failed += 1
                        if not skipped:
                            self.engines[engineRank].failed += 1
                        assert self.barriers == [] or tinfo.taskId < self.barriers[0].taskInfo.taskId
                        if self.currentReturnCode == 0:
                            # Remember the first failure. Somewhat arbitrary.
                            self.currentReturnCode = rc

                    if self.failed and self.failFast:
                        logger.info(f'Failing fast, task exited with code: {self.currentReturnCode}')
                        print('Quitting early due to task failure with --fail-fast', file=sys.stderr)
                        self.ageQ.put('CheckFailExit')
                        # Break out of the main driver control loop and drop into the exit code
                        break

                    # Maybe we want to track results by streamIndex instead of taskId?  But then there could be more than
                    # one per key
                    if self.trackResults:
                        logging.debug('Tracking result key: %r, value: %s', self.trackResults, tReport)
                        # Maintain two data structures in KVS:
                        #    self.trackResults + <task id>: a table that maps from a finished task id to a json encoding of task information including return info
                        #    self.trackResults + ' done tasks': a set of task ids that have finished
                        rd = tReport.reportDict()
                        rd['TaskCmd'] = rd['TaskCmd'].decode('utf-8', 'replace')
                        self.kvs.put(self.trackResults + f' {tinfo.taskId}'.encode(), json.dumps(rd), b'JSON')
                        self.kvs.put(self.trackResults + b' done tasks', str(tinfo.taskId), False)
                    if self.db_info.args.mailTo and self.finished % self.db_info.args.mailFreq == 0:
                        self.sendNotification()
            else:
                raise Exception('Weird message: ' + msg)

            if self.barriers:
                # Check if barrier is done.
                for x in range(retired + 1, self.barriers[0].taskInfo.taskId):
                    if x not in finishedTasks:
                        retired = x - 1
                        logger.debug('Barrier waiting for %d (%d)', x, self.barriers[0].taskInfo.taskId)
                        break
                else:
                    # We could prune finishedTasks at this point.
                    bReport = self.barriers.pop(0)
                    bTinfo = bReport.taskInfo
                    finishedTasks[bTinfo.taskId] = True

                    retired = bTinfo.taskId
                    logger.info('Finished barrier %d: %s.', retired, bTinfo)
                    if bTinfo.kind == 'D':
                        self.tasksDone = True
                        continue
                    bReport.end = time.time()
                    self.recordResult(bReport)
                    assert 0 == len(pending)

                    # If user specified a KVS key, use it to signal the barrier is done.
                    if bTinfo.bKey:
                        logger.info('put %s: %d.', bTinfo.bKey, bTinfo.taskId)
                        self.kvs.put(bTinfo.bKey, str(bTinfo.taskId), False)
                    if bTinfo.kind == 'C' and self.currentReturnCode:
                        # A "check" barrier fails if any tasks before it do (since the start or the last barrier).
                        logger.info('Barrier check failed: %d.', self.currentReturnCode)
                        self.ageQ.put('CheckFailExit')
                        # Break out of the main driver control loop and drop into the exit code
                        break
                    # Let the feeder know.
                    self.ageQ.put(bTinfo.taskId)

                    if self.barriers:
                        # clearing this barrier may clear the next
                        self.kvs.put('.controller', ('clearing barriers', None))
                    # Slight change: this tracks failures since start of last barrier
                    self.currentReturnCode = 0
                    bTinfo = None

            while pending and freeCylinders:
                ckey = freeCylinders.pop()
                if self.engines[cylKey2eRank[ckey]].status != 'running':
                    continue
                assignedCylinders.add(ckey)
                tinfo = pending.pop(0)
                logger.info('Giving %s %s', ckey, tinfo)
                self.kvs.put(ckey, ('task', tinfo))
                now = time.time()
                outstanding[tinfo.taskId] = [tinfo, ckey, now, now]
                e = self.engines[cylKey2eRank[ckey]]
                e.assigned += 1
                cRank2taskCount[e.cRank] += 1
                limit = self.contexts[e.cRank].args.context_task_limit
                if limit and cRank2taskCount[e.cRank] == limit:
                    logger.info('Context %d reached task limit %d', e.cRank, limit)
                    self.stopContext(e.cRank)

            if self.noMoreTasks and not pending and not notifiedAllDone:
                # Really nothing more to do.
                notifiedAllDone = True
                for ckey in assignedCylinders.union(freeCylinders):
                    logger.info('Notifying "%s" there is no more work.', ckey)
                    self.engines[cylKey2eRank[ckey]].dropCylinder(ckey)

            # Make changes visible via KVS.
            self.updateStatus(len(assignedCylinders) + len(freeCylinders))

        logger.info('Driver done')
        self.statusFile.close()
        self.feeder.join(3)
        if self.feeder.is_alive():
            logger.info('Exiting even though feeder is still alive.')
        self.kvs.close()


##################################################################### ENGINE


# A simple class to count the number of bytes from a file stream (e.g., pipe),
# and possibly collect the first and/or last few bytes of it
class OutputCollector(Thread):
    def __init__(self, pipe, takeStart=0, takeEnd=0):
        super().__init__(name='OutputCollector')
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
                        self.dataEnd = self.dataEnd[-end + len(r) :] + r
            if not r:
                return
            self.bytes += len(r)

    def stop(self):
        self.join(5)
        try:
            # In case someone still has the pipe open, close our end to force this thread to stop
            os.close(self.pipefd)
        except OSError:
            pass

    def data(self):
        s = self.dataStart
        if self.dataEnd:
            s += b'...' + self.dataEnd
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
    class FetchTask:
        def __init__(self, keyTemp, keyGen, kvsOp):
            self.keyTemp, self.keySeq, self.kvsOp = keyTemp, keyGen(keyTemp), kvsOp

        def fetch(self, kvs):
            return self.kvsOp(kvs, next(self.keySeq))

    class Cylinder(Thread):
        def __init__(self, context, env, envres, kvs, hbQueue, engineRank, cylinderRank, fetchTask):
            super(EngineBlock.Cylinder, self).__init__()
            self.daemon = True
            self.context, self.hbQueue, self.engineRank, self.cylinderRank, self.fetchTask = (
                context,
                hbQueue,
                engineRank,
                cylinderRank,
                fetchTask,
            )
            self.localEnv = env.copy()
            self.localEnv['DISBATCH_CORES_PER_TASK'] = str(self.context.cores_per_cylinder[self.context.nodeId])
            logger.info(
                'Cylinder %d initializing, %s cores', self.cylinderRank, self.localEnv['DISBATCH_CORES_PER_TASK']
            )
            for v, L in envres.items():
                try:
                    self.localEnv[v] = L[
                        0 if -1 == cylinderRank else cylinderRank
                    ]  # Allow the per engine cylinder to access cylinder
                # 0's resources. TODO: Ensure some sort of lock?
                except IndexError:
                    # safer to set it empty than delete it for most cases
                    self.localEnv[v] = ''
            self.shuttingDown = False
            self.taskProc = None
            self.kvs = kvs.clone()
            self.start()

        def run(self):
            logger.info('Cylinder %d in run', self.cylinderRank)
            # signal.signal(signal.SIGTERM, lambda s, f: sys.exit(1))
            try:
                self.main()
            except OSError as e:
                if not self.shuttingDown:
                    logger.info('Cylinder %d got socket error %r', self.cylinderRank, e)
            except Exception:
                logger.exception('Cylinder %d exception: ', self.cylinderRank)
            finally:
                logger.info('Cylinder %d stopping.', self.cylinderRank)
                killPatiently(self.taskProc, f'cylinder {self.cylinderRank:d} subproc', 2)

        def main(self):
            self.pid = os.getpid()  # TODO: Remove
            self.pgid = os.getpgid(0)
            logger.info('Cylinder %d firing, %d, %d.', self.cylinderRank, self.pid, self.pgid)
            if self.cylinderRank != -1:
                self.kvs.put(
                    '.controller',
                    ('cylinder available', (self.engineRank, self.pid, self.pgid, self.fetchTask.keyTemp)),
                )

            self.localEnv['DISBATCH_CYLINDER_RANK'], self.localEnv['DISBATCH_ENGINE_RANK'] = (
                str(self.cylinderRank),
                str(self.engineRank),
            )
            while 1:
                logger.info('Wating for %s', self.fetchTask.keyTemp)
                msg, ti = self.fetchTask.fetch(self.kvs)
                logger.info('%s got %s %s', self.fetchTask.keyTemp, msg, ti)
                if msg == 'stop':
                    logger.info('Cylinder %d received %s, exiting.', self.cylinderRank, msg)
                    if self.cylinderRank != -1:
                        self.kvs.put('.controller', ('cylinder stopped', (self.engineRank, self.fetchTask.keyTemp)))
                    self.shuttingDown = True
                    break

                self.localEnv['DISBATCH_STREAM_INDEX'] = str(ti.taskStreamIndex)
                self.localEnv['DISBATCH_REPEAT_INDEX'] = str(ti.taskRepIndex)
                self.localEnv['DISBATCH_TASKID'] = str(ti.taskId)
                self.localEnv['DISBATCH_STREAM_INDEX_ZP'] = f'{ti.taskStreamIndex:06d}'
                self.localEnv['DISBATCH_REPEAT_INDEX_ZP'] = f'{ti.taskRepIndex:06d}'
                self.localEnv['DISBATCH_TASKID_ZP'] = f'{ti.taskId:06d}'

                logger.info('Cylinder %d executing %s.', self.cylinderRank, ti)
                t0 = time.time()
                try:
                    self.taskProc = self.context.run_task(self.cylinderRank, ti, self.localEnv)
                    obp = OutputCollector(self.taskProc.stdout, 40, 40)
                    ebp = OutputCollector(self.taskProc.stderr, 40, 40)
                    ct = 0.0
                    pollInterval = 1  # in seconds.
                    while True:
                        # Popen.wait with timeout is resource intensive, so let's roll our own.
                        r = self.context.poll_task(self.taskProc)
                        if r is not None:
                            break
                        time.sleep(pollInterval)
                        ct += pollInterval
                        if ct >= PulseTime:
                            # We won't track hb info for per engine tasks, since they may occur multiple times, so don't need a real taskId for those.
                            self.hbQueue.put((self.engineRank, -1 if self.cylinderRank == -1 else ti.taskId))
                            ct = 0.0
                    pid = self.taskProc.pid
                    self.taskProc = None
                    t1 = time.time()

                    obp.stop()
                    ebp.stop()
                    tr = TaskReport(
                        ti,
                        self.context.node,
                        pid,
                        r,
                        t0,
                        t1,
                        obp.bytes,
                        obp.data().decode('utf-8', 'replace'),
                        ebp.bytes,
                        ebp.data().decode('utf-8', 'replace'),
                    )
                except Exception as e:
                    self.taskProc = None
                    t1 = time.time()
                    estr = 'Exception during task execution: ' + str(e) + traceback.format_exc()
                    tr = TaskReport(ti, self.context.node, -1, getattr(e, 'errno', 200), t0, t1, 0, '', len(estr), estr)

                self.kvs.put(
                    '.controller', ('task done', (tr, self.engineRank, self.cylinderRank, self.fetchTask.keyTemp))
                )

                logger.info('Cylinder %s completed: %s', self.cylinderRank, tr)

    def __init__(self, kvs, context, rank):
        super().__init__(name='EngineBlock')
        self.daemon = True
        self.context = context
        self.hbQueue = Queue()
        self.rank = rank
        cylinders = context.cylinders[context.nodeId]

        env = os.environ
        envres = {}
        for v in context.envres:
            e = env.get(v)
            if e:
                L = e.split(',')
                if len(L) < cylinders:
                    logger.error(
                        'Requested envres variable "%s" has too few values, decreasing cylinders to match: %s', v, e
                    )
                    # This may not be safe: driver is still feeding tasks based on original count
                    cylinders = len(L)
                elif len(L) > cylinders:
                    logger.warning(
                        'Requested envres variable "%s" has too many values, so some resources will not be used: %s',
                        v,
                        e,
                    )
                envres[v] = L
            else:
                logger.warning('Requested envres variable "%s" not found', v)

        self.kvs = kvs.clone()
        self.kvs.put('.controller', ('engine started', (self.rank, context.rank, myHostname, myPid, time.time())))
        env.update(self.kvs.view('.common env'))

        def indexKeyGen(template):
            c = 0
            while 1:
                yield template % c
                c += 1

        def constantKeyGen(template):
            while 1:
                yield template

        logger.info('Engine %d running start tasks', self.rank)
        peStart = self.Cylinder(
            context,
            env,
            envres,
            kvs,
            self.hbQueue,
            self.rank,
            -1,
            self.FetchTask('.per engine START %d', indexKeyGen, kvsstcp.KVSClient.view),
        )
        self.joinWithHB(peStart)
        logger.info('Engine %d completed start tasks', self.rank)

        logger.info('Engine %d running normal tasks, %d-way concurrency', self.rank, cylinders)
        self.cylinders = [
            self.Cylinder(
                context,
                env,
                envres,
                kvs,
                self.hbQueue,
                self.rank,
                x,
                self.FetchTask(f'.cylinder {self.rank:d} {x:d}', constantKeyGen, kvsstcp.KVSClient.get),
            )
            for x in range(cylinders)
        ]
        self.finished, self.inFlight, self.liveCylinders = 0, 0, len(self.cylinders)
        self.start()
        self.joinWithHB(self)
        logger.info('Engine %d completed normal tasks', self.rank)

        logger.info('Engine %d running stop tasks', self.rank)
        peStop = self.Cylinder(
            context,
            env,
            envres,
            kvs,
            self.hbQueue,
            self.rank,
            -1,
            self.FetchTask('.per engine STOP %d', indexKeyGen, kvsstcp.KVSClient.view),
        )
        self.joinWithHB(peStop)
        logger.info('Engine %d completed stop tasks', self.rank)

        self.kvs.put('.controller', ('engine stopped', (self.finalStatus, self.rank)))
        self.kvs.close()

    def joinWithHB(self, thr):
        while True:
            thr.join(timeout=PulseTime)
            if not thr.is_alive():
                break
            # We are still running, collect and transmit heart beat data.
            hbs = []
            while True:
                try:
                    o = self.hbQueue.get(block=False)
                    hbs.append(o)
                except Empty:
                    break
            self.kvs.put('.controller', ('engine heart beats', (self.rank, hbs)))

    def run(self):
        # TODO: not currently checking for a per engine clean up
        # task. Probably need to explicitly join pec, which means
        # sending that a shutdown message too.
        try:
            for c in self.cylinders:
                c.join()
            self.finalStatus = 'OK'
        except Exception as e:
            logger.exception('EngineBlock')
            self.finalStatus = str(e)


##################################################################### MAIN


# Common arguments for normal with context and context only invocations.
def contextArgs(argp: argparse.ArgumentParser):
    argp.add_argument(
        '-C',
        '--context-task-limit',
        type=int,
        metavar='TASK_LIMIT',
        default=0,
        help='Shutdown after running COUNT tasks (0 => no limit).',
    )
    argp.add_argument(
        '-c',
        '--cpusPerTask',
        metavar='N',
        default=-1.0,
        type=float,
        help='Number of cores used per task; may be fractional (default: 1).',
    )
    argp.add_argument(
        '-E', '--env-resource', metavar='VAR', action='append', default=[], help=argparse.SUPPRESS
    )  #'Assign comma-delimited resources specified in environment VAR across tasks (count should match -t)'
    argp.add_argument(
        '--fill', action='store_true', help='Try to use extra cores if allocated cores exceeds requested cores.'
    )
    argp.add_argument('-g', '--gpu', action='store_true', help=argparse.SUPPRESS)  # deprecated
    argp.add_argument(
        '--no-retire',
        dest='retire_cmd',
        action='store_const',
        const='',
        help="Don't retire nodes from the batch system (e.g., if running as part of a larger job).",
    )
    argp.add_argument('-l', '--label', type=str, metavar='COMMAND', help='Label for this context. Should be unique.')
    argp.add_argument(
        '--retire-cmd',
        type=str,
        metavar='COMMAND',
        help='Shell command to run to retire a node (environment includes $NODE being retired, remaining $ACTIVE node list, $RETIRED node list; default based on batch system). Incompatible with "--ssh-node".',
    )
    argp.add_argument(
        '-s',
        '--ssh-node',
        type=str,
        action='append',
        metavar='HOST:CORECOUNT',
        help='Run tasks over SSH on the given nodes (can be specified multiple times for additional hosts; equivalent to setting DISBATCH_SSH_NODELIST)',
    )
    argp.add_argument(
        '-t',
        '--tasksPerNode',
        metavar='N',
        default=-1,
        type=int,
        help='Maximum concurrently executing tasks per node (up to cores/cpusPerTask).',
    )

    return [
        'context_task_limit',
        'cpusPerTask',
        'env_resource',
        'fill',
        'gpu',
        'retire_cmd',
        'label',
        'ssh_node',
        'tasksPerNode',
    ]


def main(kvsq=None):
    global DbUtilPath
    global logger

    if len(sys.argv) > 1 and sys.argv[1] == '--engine':
        argp = argparse.ArgumentParser(description='Task execution engine.')
        argp.add_argument('--engine', action='store_true', help='Run in execution engine mode.')
        argp.add_argument('--method', type=str, help='Context specific start up.')
        argp.add_argument('--tag', type=str, help='Random indentifier shared by sibling task servers and their engine.')
        argp.add_argument('-n', '--node', type=str, help='Name of this engine node.')
        argp.add_argument('kvsKey', help='Key for my context.')
        args = argp.parse_args()

        if args.method:
            method_state = eval(args.method + '_start')(
                args.tag
            )  # opaque object that will be used to update context instance state.

        # Stagger start randomly to throttle kvs connections
        time.sleep(random.random() * 5.0)
        kvsserver = os.environ['DISBATCH_KVSSTCP_HOST']
        kvs = kvsstcp.KVSClient(kvsserver)
        context = kvs.view(args.kvsKey)
        if args.method:
            context.__getattribute__(args.method.split('.', 1)[-1] + '_state')(method_state)

        rank = register(kvs, 'engine')
        if -1 == rank:
            print('Run done, engine not registering.', file=sys.stderr)
            sys.exit(0)
        dbInfo = kvs.view('.db info')
        try:
            os.chdir(dbInfo.wd)
        except Exception:
            print(f'Failed to change working directory to "{dbInfo.wd}".', file=sys.stderr)
            # TODO: Fail here?

        context.setNode(args.node)
        logger = logging.getLogger('DisBatch Engine')
        lconf = {'format': '%(asctime)s %(levelname)-8s %(name)-15s: %(message)s', 'level': dbInfo.args.loglevel}
        lconf['filename'] = f'{dbInfo.uniqueId:s}_{context.label:s}_{args.node:s}_engine_{rank:d}.log'
        logging.basicConfig(**lconf)
        logger.info('Starting engine %s (%d) on %s (%d) in %s.', context.node, rank, myHostname, myPid, os.getcwd())
        logger.info('argv: %r', sys.argv)
        logger.info('args: %r', args)
        logger.info('Env: %r', os.environ)

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
        except OSError as r:
            logger.info('got socket error waiting on shutdown: %r', r)
        except Exception as e:
            logger.exception('EngineBlock during join.')
        finally:
            shutdown()

        if args.method:
            context.__getattribute__(args.method.split('.', 1)[-1] + '_stop')()

        kvs.close()
        logger.info(
            'Remaining processes:\n' + SUB.check_output(['ps', 'fuhx', '--cols', '1000']).decode('utf-8', 'ignore')
        )
        sys.exit(0)
    elif len(sys.argv) > 1 and sys.argv[1] == '--context':
        argp = argparse.ArgumentParser(description='Set up disBatch execution context')
        argp.add_argument('--context', action='store_true', help=argparse.SUPPRESS)
        argp.add_argument('dbutilpath')
        commonContextArgs = contextArgs(argp)
        args = argp.parse_args()

        DbUtilPath = args.dbutilpath

        kvsserver = os.environ['DISBATCH_KVSSTCP_HOST']
        kvs = kvsstcp.KVSClient(kvsserver)
        dbInfo = kvs.view('.db info')

        # Args that if not set might have been set when disBatch was first run.
        if args.cpusPerTask == -1.0:
            args.cpusPerTask = dbInfo.args.cpusPerTask
        if args.tasksPerNode == -1:
            args.tasksPerNode = dbInfo.args.tasksPerNode
        if args.env_resource == []:
            args.env_resource = dbInfo.args.env_resource

        rank = register(kvs, 'context')
        if -1 == rank:
            print('Run done, context not registering.', file=sys.stderr)
            sys.exit(0)
        try:
            os.chdir(dbInfo.wd)
        except Exception:
            print(f'Failed to change working directory to "{dbInfo.wd}".', file=sys.stderr)
            # TODO: Fail here?

        # Try to find a batch context.
        if args.ssh_node:
            context = SSHContext(dbInfo, rank, args)
        else:
            context = probeContext(dbInfo, rank, args)
        if not context:
            print('Cannot determine batch execution environment.', file=sys.stderr)
            sys.exit(1)

        logger = logging.getLogger('DisBatch Context')
        lconf = {'format': '%(asctime)s %(levelname)-8s %(name)-15s: %(message)s', 'level': dbInfo.args.loglevel}
        lconf['filename'] = f'{dbInfo.uniqueId}_{context.label}.context.log'
        logging.basicConfig(**lconf)
        logging.info('%s context started on %s (%d).', context.sysid, myHostname, myPid)
        logger.info('argv: %r', sys.argv)
        logger.info('args: %r', args)
        logger.info('Env: %r', os.environ)
        # Log any messages generated by the context constructor.
        for m, ll in getattr(context, 'for_log', []):
            if ll is context.USERWARNING:
                warnings.warn(m)
                ll = logging.WARNING
            logger.log(ll, '(Delayed from __init__): ' + m)
        nogo = [x for x, c in enumerate(context.cylinders) if c == 0]
        if nogo:
            logger.warning(
                'At least one engine lacks enough cylinders to run tasks (%r).' % ([context.nodes[x] for x in nogo])
            )
        if len(nogo) == len(context.cylinders):
            logger.error('No viable engines.')
            sys.exit(1)

        if args.gpu:
            print('-g/--gpu is deprecated. It is no longer needed and will be ignored.', file=sys.stderr)
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
        # TODO: python -m disbatch shows __main__.py in the help message. Does click do this?
        argp = argparse.ArgumentParser(description='Use batch resources to process a file of tasks, one task per line.')
        argp.add_argument(
            '-e',
            '--exit-code',
            action='store_true',
            help='When any task fails, exit with non-zero status (default: only if disBatch itself fails)',
        )
        argp.add_argument(
            '--force-resume', action='store_true', help='With -r, proceed even if task commands/lines are different.'
        )
        argp.add_argument('--kvsserver', nargs='?', default=True, metavar='HOST:PORT', help='Use a running KVS server.')
        argp.add_argument('--logfile', metavar='FILE', default=None, type=argparse.FileType('w'), help='Log file.')
        argp.add_argument(
            '--loglevel',
            choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
            default='INFO',
            help='Logging level (default: INFO).',
        )
        argp.add_argument(
            '--mailFreq',
            default=None,
            type=int,
            metavar='N',
            help='Send email every N task completions (default: 1). "--mailTo" must be given.',
        )
        argp.add_argument(
            '--mailTo', metavar='ADDR', default=None, help='Mail address for task completion notification(s).'
        )
        argp.add_argument(
            '-p',
            '--prefix',
            metavar='PATH',
            default='.',
            help='Path for log, dbUtil, and status files (default: "."). If ends with non-directory component, use as prefix for these files names (default: <Taskfile>_disBatch_<YYYYMMDDhhmmss>_<Random>).',
        )
        argp.add_argument(
            '-r',
            '--resume-from',
            metavar='STATUSFILE',
            action='append',
            help='Read the status file from a previous run and skip any completed tasks (may be specified multiple times).',
        )
        argp.add_argument(
            '-R',
            '--retry',
            action='store_true',
            help='With -r, also retry any tasks which failed in previous runs (non-zero return).',
        )
        argp.add_argument(
            '-S',
            '--startup-only',
            action='store_true',
            help='Startup only the disBatch server (and KVS server if appropriate). Use "dbUtil..." script to add execution contexts. Incompatible with "--ssh-node".',
        )  # TODO: Add addDBExecContext file name override?
        argp.add_argument('--status-header', action='store_true', help='Add header line to status file.')
        argp.add_argument(
            '--use-address', default=None, metavar='HOST:PORT', help='Specify hostname and port to use for this run.'
        )
        argp.add_argument('-w', '--web', action='store_true', help='Enable web interface.')
        argp.add_argument(
            '-f',
            '--fail-fast',
            action='store_true',
            help='Exit on first task failure. Running tasks will be interrupted and disBatch will exit with a non-zero exit code.',
        )
        source = argp.add_mutually_exclusive_group(required=True)
        source.add_argument(
            '--taskcommand',
            default=None,
            metavar='COMMAND',
            help='Tasks will come from the command specified via the KVS server (passed in the environment).',
        )
        source.add_argument(
            '--taskserver', nargs='?', default=False, metavar='HOST:PORT', help='Tasks will come from the KVS server.'
        )
        source.add_argument(
            'taskfile',
            nargs='?',
            default=None,
            type=argparse.FileType('rb'),
            help='File with tasks, one task per line ("-" for stdin)',
        )  # TODO: Change "-" remark?
        source.add_argument('--version', action='store_true', help='Print the version and exit')
        commonContextArgs = contextArgs(argp)
        args = argp.parse_args()

        if args.version:
            from . import __version__

            print(__version__)
            sys.exit(0)

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
        elif args.taskserver is None:  # --taskserver with no argument
            args.taskserver = args.kvsserver
        elif args.taskserver and args.kvsserver != args.taskserver:
            print('Cannot use different --kvsserver and --taskservers.', file=sys.stderr)
            sys.exit(1)

        try:
            tfn = os.path.basename(args.taskfile.name).strip('<>')
        except AttributeError:
            tfn = 'STREAM'

        forceDir = args.prefix[-1] == '/'
        rp = os.path.realpath(args.prefix)
        if os.path.isdir(rp):
            uniqueId = rp + '/{:s}_disBatch_{:s}_{:03d}'.format(
                tfn, time.strftime('%y%m%d%H%M%S'), int(random.random() * 1000)
            )
        else:
            if not forceDir:
                rpp, name = os.path.split(rp)
            else:
                rpp, name = rp, ''  # By design, this will trigger the error exit.
            if not os.path.isdir(rpp):
                print(f'Directory {rpp} does not exist.', file=sys.stderr)
                sys.exit(1)
            uniqueId = rp

        logger = logging.getLogger('DisBatch')
        args.loglevel = getattr(logging, args.loglevel)
        lconf = {'format': '%(asctime)s %(levelname)-8s %(name)-15s: %(message)s', 'level': args.loglevel}
        if args.logfile:
            args.logfile.close()
            lconf['filename'] = args.logfile.name
        else:
            lconf['filename'] = uniqueId + '_driver.log'
        logging.basicConfig(**lconf)
        logger.info('Starting feeder (%d) on %s in %s.', myPid, myHostname, os.getcwd())
        logger.info('Args: %r', sys.argv)
        logger.info('Env: %r', os.environ)

        if args.kvsserver is True:
            # start our own
            if args.use_address:
                host, port = args.use_address.split(':')
                port = int(port)
            else:
                host, port = socket.gethostname(), 0
            kvsst = kvsstcp.KVSServerThread(host, port)
            kvsserver = '{:s}:{:d}'.format(*kvsst.cinfo)
            kvsinfotxt = uniqueId + '_kvsinfo.txt'
            with open(kvsinfotxt, 'w') as kvsi:
                kvsi.write(kvsserver)
            kvsenv = kvsst.env()
        else:
            if args.use_address:
                logger.warning(
                    f'--use-hostname={args.use_address} will be ignored. disBatch is not starting a KVS server.'
                )
            # use one given (possibly via environment)
            kvsst = None
            kvsserver = args.kvsserver
            kvsenv = None

        logger.info('KVS Server: %s', kvsserver)
        if kvsq:
            # If kvsq is not None, that means disBatch is being
            # started programmatically (in a new thread).  kvsq will
            # be used to communicate KVS contact info back to the code
            # doing the start up.
            kvsq.put(kvsserver)

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
        resultKey = None
        if args.taskfile:
            taskSource = args.taskfile
        else:
            taskSource = KVSTaskSource(kvs)
            if args.taskcommand:
                logger.info('Tasks will come from: ' + repr(args.taskcommand))
                if kvsq is None:
                    taskProcess = TaskProcess(taskSource, args.taskcommand, shell=True, env=kvsenv, close_fds=True)
                taskSource.waitForSignIn()
                resultKey = taskSource.resultkey
                logger.info(
                    'Task source name: ' + taskSource.name.decode('utf-8')
                )  # TODO: Think about the decoding a bit more?

        def resumefilter(t):
            return statusTaskFilter(t, parseStatusFiles(*args.resume_from), args.retry, args.force_resume)

        tasks = TaskGenerator(taskSource, filter=resumefilter if args.resume_from else None)

        if tasks.done:
            print('No tasks to run.', file=sys.stderr)

        if args.web:
            from kvsstcp import wskvsmu

            urlfile = uniqueId + '_url'
            wskvsmu.main(kvsserver, urlfile=open(urlfile, 'w'), monitorspec=':gpvw')

        DbUtilPath = f'{uniqueId}_dbUtil.sh'
        dbutil_template = importlib.resources.files('disbatch').joinpath('dbUtil.template.sh').read_text()

        # As a convenience to the user, we would like them to be able to run dbUtil.sh
        # during their job to track the status even if disBatch isn't in their environment.
        # To facilitate that, we use the Python executable that disBatch was invoked with.
        DisBatchPython = sys.executable

        with open(DbUtilPath, 'w', opener=partial(os.open, mode=0o700)) as fd:
            fd.write(
                dbutil_template.format(
                    DisBatchPython=DisBatchPython, DbUtilPath=DbUtilPath, kvsserver=kvsserver, uniqueId=uniqueId
                )
            )

        subContext = None
        if not tasks.done:
            # Don't start the context if there are no tasks to run,
            # the kvs is about to shut down so the context won't be able to connect.

            if not args.startup_only:
                # Is there a cleaner way to do this?
                extraArgs = []
                argsD = args.__dict__
                for name in commonContextArgs:
                    v = argsD[name]
                    if v is None:
                        continue
                    aName = '--' + name.replace('_', '-')
                    if isinstance(v, bool):
                        if v:
                            extraArgs.append(aName)
                    elif isinstance(v, list):
                        for e in v:
                            extraArgs.extend([aName, str(e)])
                    else:
                        extraArgs.extend([aName, str(v)])

                subContext = SUB.Popen(
                    [DbUtilPath] + extraArgs,
                    stdin=open(os.devnull),
                    stdout=open(uniqueId + '_context_wrap.out', 'w'),
                    close_fds=True,
                )
            else:
                print('Run this script to add compute contexts:\n   ' + DbUtilPath)

        driver = Driver(kvs, dbInfo, tasks, getattr(taskSource, 'resultkey', resultKey))
        try:
            while driver.is_alive():
                if taskProcess and taskProcess.r:
                    logger.error('Task generator failed; forcing shutdown')
                    sys.exit(taskProcess.r)
                if subContext and subContext.poll():
                    # TODO: Add a flag to control this behavior?
                    logger.error('Context exited; forcing shutdown')
                    sys.exit(subContext.returncode)
                driver.join(PulseTime)
                kvs.put('.controller', ('driver heart beat', None))
        except Exception as e:
            logger.exception('Watchdog')
        finally:
            if kvsst:
                logger.info('Shutting down KVS server.')
                kvs.shutdown()
            else:
                kvs.close()
            if args.kvsserver is True:
                try:
                    os.unlink(kvsinfotxt)
                except OSError:
                    # may have been already removed -- can happen if multiple disbatch runs in same directory
                    pass
            if args.web:
                os.unlink(urlfile)
            if subContext:
                killPatiently(subContext, 'Execution context')
        if subContext and subContext.returncode:
            print('Some engine processes failed -- please check the logs', file=sys.stderr)
            sys.exit(1)

        if args.exit_code and driver.failed:
            print('Some tasks failed with non-zero exit codes -- please check the logs', file=sys.stderr)
            sys.exit(1)
