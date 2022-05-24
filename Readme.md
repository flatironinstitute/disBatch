Distributed processing of a batch of tasks.
===========================================

## Overview

One common usage pattern for distributed computing involves processing a
long list of commands (aka *tasks*):

    ( cd /path/to/workdir ; source SetupEnv ; myprog -a 0 -b 0 -c 0 ) &> task_0_0_0.log
    ( cd /path/to/workdir ; source SetupEnv ; myprog -a 0 -b 0 -c 1 ) &> task_0_0_1.log
    ...
    ( cd /path/to/workdir ; source SetupEnv ; myprog -a 9 -b 9 -c 8 ) &> task_9_9_8.log
    ( cd /path/to/workdir ; source SetupEnv ; myprog -a 9 -b 9 -c 9 ) &> task_9_9_9.log



One could do this by submitting 1,000 separate jobs to a cluster computer, but that may
present problems for the queuing system and can behave badly if the
system is configured to handle jobs in a simple first come, first serve
fashion.

Another alternative is to use a resource management system's native
support for job arrays, but this often requires massaging the commands
to reflect syntax specific to a particular system's implementation of
job arrays.

And what if you don't have a cluster available, but do have a collection of networked computers? Or you just want to make use of multiple cores on your own computer?

In any event, when processing such a list of tasks, it is helpful to
acquire metadata about the execution of each task: where it ran, how
long it took, its exit return code, etc.

**disBatch** has been designed to support this usage in a simple and
portable way, as well as to provide the sort of metadata that can be
helpful for debugging and reissuing failed tasks.

It can take as input a file, each of whose lines is a task in the form of a
command sequence. For example, the file could consists of the 1000 commands listed above. It launches the tasks one
after the other until all specified execution resources are in use. Then as one
executing task exits, the next task in the file is launched. This repeats until all
the lines in the file have been processed.

Each task is run in a new shell. If you want to manipulate the execution environment of a task, add the appropriate operations to the command sequence&mdash;`source SetupEnv` in the above is one example. For another, if you just need to set an environment variable, each task line would look something like:

    export PYTHONPATH=/d0/d1/d2:$PYTHONPATH ; rest ; of ; command ; sequence
    
Or, for more complex set ups, command sequences and input/output redirection requirements, you could place everything in a small shell script with appropriate arguments for the parts that vary from task to task, say RunMyprog.sh:

    #!/bin/bash
    
    id=$1
    shift
    cd /path/to/workdir
    module purge
    module load gcc python3
    
    export PYTHONPATH=/d0/d1/d2:$PYTHONPATH
    myProg "$@" > results/${id}.out 2> logs/${id}.log

The task file would then contain:

    ./RunMyprog.sh 0_0_0 -a 0 -b 0 -c 0
    ./RunMyprog.sh 0_0_1 -a 0 -b 0 -c 1
    ...
    ./RunMyprog.sh 9_9_8 -a 9 -b 9 -c 8
    ./RunMyprog.sh 9_9_9 -a 9 -b 9 -c 9

See \#DISBATCH directives below for ways to simplify task lines.

Once you have created the task file, running disBatch is straightforward. For example, working with a cluster managed by SLURM,
all that needs to be done is to submit a job like the following:

    sbatch -n 20 --ntasks-per-node 5 disBatch TaskFileName

This particular invocation will allocate sufficient resources to process
20 tasks at a time, with no more than five running concurrently on any
given node. disBatch will use environment variables initialized by SLURM to determine the execution resources to use for the run.
This invocation assumes an appropriately installed disBatch is in your PATH, see below for installation notes.

Various log files will be created as the run unfolds:

* `TaskFileName_134504_status.txt` (assuming the SLURM Job ID is `134504`): status of every task (details below)
* `disBatch_134504_driver.txt` (can be changed with `-l`), `TaskFileName_134504_worker032_engine.txt`: 
  The disBatch log file contains details mostly of interest in case of a
  problem with disBatch itself. It can generally be ignored by end
  users (but keep it around in the event that something did go
  wrong&mdash;it will aid debugging). The ``*_engine.txt'' files contain similar information for each node acting as an execution resource
* `disBatch_134504_kvsinfo.txt`: TCP address of invoked KVS server if any (for additional advanced status monitoring)

### Status file

The `_status.txt` file contains tab-delimited lines of the form:

    314	315	-1	worker032	8016	0	10.0486528873	1458660919.78	1458660929.83	0	""	0	""	'cd /path/to/workdir ; myprog -a 3 -b 1 -c 4 > task_3_1_4.log 2>&1'

These fields are:

  1. Flags: The first field, blank in this case, may contain `E`, `O`, `R`, `B`, or `S` flags.
     Each program/task should be invoked in such a way that standard error
     and standard output end up in appropriate files. If that's not the case
     `E` or `O` flags will be raised. `R` indicates that the task
     returned a non-zero exit code. `B` indicates a barrier (see below). `S` indicates the job was skipped (this may happen during "resume" runs).
  1. Task ID: The `314` is the 0-based index of the task (starting from the beginning of the task file, incremented for each task, including repeats).
  1. Line number: The `315` is the 1-based line from the task file. Blank lines, comments, directives and repeats may cause this to drift considerably from the value of Task ID.
  1. Repeat index: The `-1` is the repeat index (as in this example, `-1` indicates this task was not part of a repeat directive).
  1. Node: `worker032` identifies the node on which the task ran.
  1. PID: `8016` is the PID of the bash shell used to run the task.
  1. Exit code: `0` is the exit code returned.
  1. Elapsed time: `10.0486528873` (seconds),
  1. Start time:`1458660919.78` (epoch based),
  1. Finish time: `1458660929.83` (epoch based).
  1. Bytes of *leaked* output (not redirected to a file),
  1. Output snippet (up to 80 bytes consisting of the prefix and suffix of the output),
  1. Bytes of leaked error output,
  1. Error snippet,
  1. Command: `cd ...` is the text of the task (repeated from the task file, but see below).

## Installation

**Users of Flatiron resources: disBatch is available via the module system. You do not need to clone this repo to use it.**

`disBatch.py` requires the `kvsstcp` package, which should be installed in python's path, or placed in this directory.
You can simply clone this git repository with `--recursive` (or run `git submodule update --init` if you've already cloned it).

Depending on your execution environment, the ability of disBatch to determine the location of itself and kvsstcp may be disrupted. To avoid such problems, set the environment variable `DISBATCH_ROOT` to the path of the directory containing the subdirectory `disbatch`. 

disBatch is designed to support a variety of execution environments, from your own desktop, to a local collection of workstations, to large clusters managed by job schedulers.
It currently supports SLURM and can be executed from `sbatch`, but it is architected to make it simple to add support for other resource managers.

You can also run directly on one or more machines by setting an environment variable:

    DISBATCH_SSH_NODELIST=localhost:7,otherhost:3

or specifying an invocation argument:

    -s localhost:7,otherhost:3
    
This allows execution directly on your `localhost` and via ssh for remote hosts without the need for a resource management system.
In this example, disBatch is told it can use seven CPUs on your local host and three on `otherhost`. Assuming the default mapping of one task to one CPU applies in this example, seven tasks could be in progress at any given time on `localhost`, and three on `otherhost`. Note that `localhost` is an actual name you can use to refer to the machine on which you are currently working. `otherhost` is fictious. 
Hosts used via ssh must be set up to allow ssh to work without a password and must share the working directory for the disBatch run.

disBatch refers to a collection of execution resources as a *context* and the resources proper as *engines*. So the earlier SLURM example created one context with four engines (capable of running five concurrent tasks each), while the SSH example created one context with two engines (capable of running seven and three concurrent tasks, respectively).

## Invocation
~~~~
usage: disBatch.py [-h] [-e] [--force-resume] [--kvsserver [HOST:PORT]]
                   [--logfile FILE] [--mailFreq N] [--mailTo ADDR] [-p PATH]
                   [-r STATUSFILE] [-R] [-S] [--status-header] [-w]
                   [--taskcommand COMMAND] [--taskserver [HOST:PORT]]
                   [-C TASK_LIMIT] [-c N] [--fill] [-g] [--no-retire]
                   [-l COMMAND] [--retire-cmd COMMAND] [-s HOST:COUNT] [-t N]
                   [taskfile]

Use batch resources to process a file of tasks, one task per line.

positional arguments:
  taskfile              File with tasks, one task per line ("-" for stdin)

optional arguments:
  -h, --help            show this help message and exit
  -e, --exit-code       When any task fails, exit with non-zero status
                        (default: only if disBatch itself fails)
  --force-resume        With -r, proceed even if task commands/lines are
                        different.
  --kvsserver [HOST:PORT]
                        Use a running KVS server.
  --logfile FILE        Log file.
  --mailFreq N          Send email every N task completions (default: 1). "--
                        mailTo" must be given.
  --mailTo ADDR         Mail address for task completion notification(s).
  -p PATH, --prefix PATH
                        Path for log, dbUtil, and status files (default: ".").
                        If ends with non-directory component, use as prefix
                        for these files names (default: TASKFILE_JOBID).
  -r STATUSFILE, --resume-from STATUSFILE
                        Read the status file from a previous run and skip any
                        completed tasks (may be specified multiple times).
  -R, --retry           With -r, also retry any tasks which failed in previous
                        runs (non-zero return).
  -S, --startup-only    Startup only the disBatch server (and KVS server if
                        appropriate). Use "dbUtil..." script to add execution
                        contexts. Incompatible with "--ssh-node".
  --status-header       Add header line to status file.
  -w, --web             Enable web interface.
  --taskcommand COMMAND
                        Tasks will come from the command specified via the KVS
                        server (passed in the environment).
  --taskserver [HOST:PORT]
                        Tasks will come from the KVS server.
context specific arguments:
  -C TASK_LIMIT, --context-task-limit TASK_LIMIT
                        Shutdown after running COUNT tasks (0 => no limit).
  -c N, --cpusPerTask N
                        Number of cores used per task; may be fractional
                        (default: 1).
  --fill                Try to use extra cores if allocated cores exceeds
                        requested cores.
  -g, --gpu             Use assigned GPU resources
  --no-retire           Don't retire nodes from the batch system (e.g., if
                        running as part of a larger job); equivalent to -k ''.
  -l COMMAND, --label COMMAND
                        Label for this context. Should be unique.
  --retire-cmd COMMAND  Shell command to run to retire a node (environment
                        includes $NODE being retired, remaining $ACTIVE node
                        list, $RETIRED node list; default based on batch
                        system). Incompatible with "--ssh-node".
  -s HOST:COUNT, --ssh-node HOST:COUNT
                        Run tasks over SSH on the given nodes (can be
                        specified multiple times for additional hosts;
                        equivalent to setting DISBATCH_SSH_NODELIST)
  -t N, --tasksPerNode N
                        Maximum concurrently executing tasks per node (up to
                        cores/cpusPerTask).
~~~~

The options for mail will only work if your computing environment permits processes to access mail via SMTP.

A value for `-c` < 1 effectively allows you to run more tasks concurrently than CPUs specified for the run. This is somewhat unusual, and generally not recommended, but could be appropriate in some cases.

The `--no-retire` and `--retire-cmd` flags allow you to control what disBatch does when a node is no longer needed to run jobs.
When running under slurm, disBatch will by default run the command:

    scontrol update JobId="$SLURM_JOBID" NodeList="${DRIVER_NODE:+$DRIVER_NODE,}$ACTIVE"

which will tell slurm to release any nodes no longer being used.
You can set this to run a different command, or nothing at all.
While running this command, the follow environment variables will be set: `NODE` (the node that is no longer needed), `ACTIVE` (a comma-delimited list of nodes that are still active), `RETIRED` (a comma-delimited list of nodes that are no longer active, including `$NODE`), and possibly `DRIVER_NODE` (the node still running the main disBatch script, if it's not in `ACTIVE`).

The `-g` argument parses the CUDA environment varables (`CUDA_VISIBLE_DEVICES`, `GPU_DEVICE_ORDINAL`) provided on each node and divides the resources between the running tasks.  For example, with slurm, if you want to run on _n_ nodes, with _t_ tasks per node, each using _c_ CPUs and 1 GPU (that is, _tc_ CPUs and _t_ GPUs per node, or _ntc_ CPUs and _nt_ GPUs total), you can do:

    sbatch -N$n -c$c --ntasks-per-node=$t --gres=gpu:$t -p gpu --wrap 'disBatch.py -g $taskfile'`

`-S` Starts disBatch in a mode in which it waits for execution resources to be added. In this mode, disBatch starts up the task management system and
generates a script `<Prefix>_dbUtil.sh`, where `<Prefix>` refers to the `-p` option or default, see above. We'll call this simply `dbUtils.sh` here,
but remember to include `<Prefix>_` in actual use. You can add execution resources by doing one or more of the following multiple times:
1. Submit `dbUtils.sh` as a job, e.g.:

    `sbatch -N 5 --ntasks-per-node 7 dbUtil.sh`

2. Use ssh, e.g.:

    `./dbUtil.sh -s localhost:4,friendlyNeighbor:5`

Each of these creates an execution context, which contains one of more execution engines (five engines in the first, two in the second).
An engine can run one or more tasks currently. In the first example, each of the five engines will run up to seven tasks concurrently, while in the
second example, the engine on `localhost` will run up to four tasks concurrently and the engine on `friendlyNeighbor` will run up to five.
`./dbUtil.sh --mon` will start a simple ASCII-based monitor that tracks the overall state of the disBatch run, and the activity of the individual
contexts and engines. By cursoring over an engine, you can send a shutdown signal to the engine or its context. This signal is *soft*, triggering
a graceful shutdown that will occur only after currently assigned tasks are complete. Other execution resources are uneffected.

When a context is started, you can also supply the argument `--context-task-limit N`. This will shutdown the context and all associated engines
after it has run `N` tasks.  

Taken together, these mechanisms enable disBatch to run on a dynamic pool of execution resources, so you can "borrow" a colleague's workstation overnight, or
claim a large chunk of a currently idle partition, but return some if demands picks up, or chain together a series of time limited allocations to
accomplish a long run. When using this mode, keep in mind two caveats: (i) The time quantum is determined by your task duration. If any given task might
run for hours or days, then the utility of this is limited. You can still use standard means (kill, scancel) to terminate contexts and engines, but
you will likely have incomplete tasks to
reckon with; (ii) The task manangement system must itself be run in a setting where a long lived process is OK. Say in a `screen` or `tmux` session on
the login node of a cluster, or on your personal workstation (assuming it has the appropriate connectivity to reach the other resources you plan to use).


`-r` uses the status file of a previous run to determine what tasks to run during this disBatch invocation. Only those tasks that haven't yet run (or with `-R`, those that haven't run or did but returned a non-0 exit code) are run this time. By default, the numeric task identifier and the text of the command are used to determine if a current task is the same as one found in the status file. `--force-resume` restricts the comparison to just the numeric identifier.

`--kvsserver`, `--taskcommand`, and `--taskserver` implement advanced functionality (placing disBatch in an existing shared key store context and allowing for a programmatic rather than textual task interface). Contact the authors for more details.


### Considerations for large runs

If you do submit jobs with order 10000 or more tasks, you should
carefully consider how you want to organize the output (and error) files
produced by each of the tasks. It is generally a bad idea to have more
than a few thousand files in any one directory, so you will probably
want to introduce at least one extra level of directory hierarchy so
that the files can be divided into smaller groups. Intermediate
directory `13`, say, might hold all the files for tasks 13000 to
13999.

## \#DISBATCH directives

### PREFIX and SUFFIX

In order to simplify task files, disBatch supports a couple of
directives to specify common task prefix strings and suffix strings. It
also provides environment variables to identify various aspects of the
submission. Here's an example

    # Note there is a space at the end of the next line.
    #DISBATCH PREFIX ( cd /path/to/workdir ; source SetupEnv ; 
    #DISBATCH SUFFIX  ) &> ${DISBATCH_NAMETASKS}_${DISBATCH_JOBID}_${DISBATCH_TASKID}.log

These are textually prepended and appended, respectively, to the text of
each subsequent task line. If the suffix includes redirection and a task is a proper command sequence (a series of
commands joined by `;`), then the task should be wrapped in `( ... )`, as in this example, so that the standard error and standard output of the whole sequence
will be redirected to the log file. If this is not done, only standard
error and standard output for the last component of the command sequence
will be captured. This is probably not what you want unless you have
redirected these outputs for the previous individual parts of the
command sequence.

Using these, the above commands could be replaced with:

    myprog -a 0 -b 0 -c 0
    myprog -a 0 -b 0 -c 1
    ... 
    myprog -a 9 -b 9 -c 8
    myprog -a 9 -b 9 -c 9

Note: the log files will have a different naming scheme, but there will still be one per task.

Later occurrences of `#DISBATCH PREFIX` or `#DISBATCH SUFFIX` in a task
file simply replace previous ones. When these are used, the tasks
reported in the status file include the prefix and suffix in
force at the time the task was launched.

### BARRIER

If your tasks fall into groups where a later group should only begin
after all tasks of the previous group have completely finished, you can
use this directive:

    #DISBATCH BARRIER

When disBatch encounters this directive, it will not launch another task
until all tasks in progress have completed. The following form:

    #DISBATCH BARRIER CHECK

checks the exit status of the tasks done since the last barrier (or
start of the run). If any task had a non-zero exit status, the run
will exit once this barrier is met.

### REPEAT

For those problems that are easily handled via a job-array-like approach:

     #DISBATCH REPEAT 5 start 100 step 50 [command]

will expand into five tasks, each with the environment variable
`DISBATCH_REPEAT_INDEX` set to one of 100, 150, 200, 250, or 300.
The tasks will consist of the concatenation of the prefix, command (if provided),
and the suffix currently in effect. `start` defaults to 0, `step`
to 1. Note: the semantics here differ somewhat from many range
constructs, the number immediately following `REPEAT` sets the
number of tasks that will be executed; the next two numbers affect
only the value that the repeat index will have in
the environment for each of the repeat task instances. So, returning to our earlier example, the task file
could be:

    #DISBATCH PREFIX  ( cd /path/to/workdir ; a=$((DISBATCH_REPEAT_INDEX/100)) b=$(((DISBATCH_REPEAT_INDEX%100)/10 )) c=$((DISBATCH_REPEAT_INDEX%10)) ; myprog -a $a -b $b -c $c ) &> task_${a}_${b}_${c}.log
    #DISBATCH REPEAT 1000

This is not a model of clarify, but does illustrate that the repeat constuct can be relatively powerful. Many users may find it more convenient to use the tool of their choice to generate a text file with 1000 invocations explictly written out.

### PERENGINE

    #DISBATCH PERENGINE START { command ; sequence ; } &> engine_start_${DISBATCH_ENGINE_RANK}.log
    #DISBATCH PERENGINE STOP { command ; sequence ; } &> engine_stop_${DISBATCH_ENGINE_RANK}.log

Use these to specify commands that should run at the time an engine joins a disBatch run or at the time the engine leaves the disBatch run, respectively.
You could, for example, use these to bulk copy some heavily referenced read-only data to the engine's local storage area before any tasks are run, and then delete that data when the engine shuts down.
You can use the environment variable DISBATCH_ENGINE_RANK to distinguish one engine from another; for example, it is used here to keep log files separate.

These directives must come before any other tasks.

## License

Copyright 2017 Simons Foundation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
