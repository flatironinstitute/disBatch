# disBatch

Distributed processing of a batch of tasks.

[![Tests](https://github.com/flatironinstitute/disBatch/actions/workflows/tests.yaml/badge.svg)](https://github.com/flatironinstitute/disBatch/actions/workflows/tests.yaml)

## Quickstart

Install with pip:

    pip install disbatch

Create a file `Tasks` with a list of commands you want to run. These should be Bash commands like one would run on the command line:

    myprog arg0 &> myprog_0.log
    myprog arg1 &> myprog_1.log
    ...
    myprog argN &> myprog_N.log

Then, to run 5 tasks at a time in parallel on your local machine, run:

    disBatch -s localhost:5 Tasks

`disBatch` will start the first five running concurrently. When one finishes, the next will be started until all are done.

Or, to distribute this work on a Slurm cluster, run:

    sbatch -n 5 disBatch Tasks

You may need to provide additional arguments specific to your cluster to specify a partition, time limit, etc.
  
## Overview

One common usage pattern for distributed computing involves processing a
long list of commands (aka *tasks*):

    myprog -a 0 -b 0 -c 0
    myprog -a 0 -b 0 -c 1
    ...
    myprog -a 9 -b 9 -c 9

This represents a file with 1000 lines; the `...` is just a stand-in and wouldn't literally be in the task file.

One could run this by submitting 1,000 separate jobs to a cluster, but that may
present problems for the queuing system and can behave badly if the
system is configured to handle jobs in a simple first come, first serve
fashion.  For short tasks, the job launch overhead may dominate the runtime, too.

One could simplify this by using, e.g., Slurm job arrays, but each job in a job
array is an independent Slurm job, so this suffers from the same per-job overheads
as if you submitted 1000 independent jobs. Furthermore, if nodes are being allocated
exclusively (i.e. the nodes that are allocated to your job are not shared by other jobs),
then the job array approach can hugely underutilize the compute resources unless each
task is using a full node's worth of resources.

And what if you don't have a cluster available, but do have a collection of networked computers? Or you just want to make use of multiple cores on your own computer?

In any event, when processing such a list of tasks, it is helpful to
acquire metadata about the execution of each task: where it ran, how
long it took, its exit return code, etc.

disBatch has been designed to support this usage in a simple and
portable way, as well as to provide the sort of metadata that can be
helpful for debugging and reissuing failed tasks.

It can take as input a file, each of whose lines is a task in the form of a
Bash command. For example, the file could consists of the 1000 commands listed above. It launches the tasks one
after the other until all specified execution resources are in use. Then as one
executing task exits, the next task in the file is launched. This repeats until all
the lines in the file have been processed.

Each task is run in a new shell; i.e. all lines are independent of one another. 

Here's a more complicated example, demonstrating controlling the execution environment and capturing the output of the tasks:

    ( cd /path/to/workdir ; source SetupEnv ; myprog -a 0 -b 0 -c 0 ) &> task_0_0_0.log
    ( cd /path/to/workdir ; source SetupEnv ; myprog -a 0 -b 0 -c 1 ) &> task_0_0_1.log
    ...
    ( cd /path/to/workdir ; source SetupEnv ; myprog -a 9 -b 9 -c 8 ) &> task_9_9_8.log
    ( cd /path/to/workdir ; source SetupEnv ; myprog -a 9 -b 9 -c 9 ) &> task_9_9_9.log

Each line uses standard Bash syntax. Let's break it down:

1. the `( ... ) &> task_0_0_0.log` captures all output (stdout and stderr) from any command in the parentheses and writes it to `task_0_0_0.log`;
2. `cd /path/to/workdir` changes the working directory;
3. `source SetupEnv` executes a script called `SetupEnv`, which could contain commands like `export PATH=...` or `module load ...` to set up the environment;
4. `myprog -a 0 -b 0 -c 0` is the command you want to run.

The semicolons between the last 3 statements are Bash syntax to run a series of commands on the same line.

You can simplify this kind of task file with the `#DISBATCH PREFIX` and `#DISBATCH SUFFIX` directives. See the [#DISBATCH directives](#disbatch-directives) section for full details, but here's how that could look:

    #DISBATCH PREFIX ( cd /path/to/workdir ; source SetupEnv ; myprog 
    #DISBATCH SUFFIX ) &> task_${DISBATCH_TASKID}.log
    -a 0 -b 0 -c 0
    -a 0 -b 0 -c 1
    ...
    -a 9 -b 9 -c 9


Note that for a simple environment setup, you don't need a `source SetupEnv`. You can just set an environment variable directly in the task line, as you can in Bash:

    export LD_LIBRARY_PATH=/d0/d1/d2:$LD_LIBRARY_PATH ; rest ; of ; command ; sequence
    
For more complex set ups, command sequences and input/output redirection requirements, you could place everything in a small shell script with appropriate arguments for the parts that vary from task to task, say `RunMyprog.sh`:

    #!/bin/bash
    
    id=$1
    shift
    cd /path/to/workdir
    module purge
    module load gcc openblas python3 
    
    export LD_LIBRARY_PATH=/d0/d1/d2:$LD_LIBRARY_PATH
    myProg "$@" > results/${id}.out 2> logs/${id}.log

The task file would then contain:

    ./RunMyprog.sh 0_0_0 -a 0 -b 0 -c 0
    ./RunMyprog.sh 0_0_1 -a 0 -b 0 -c 1
    ...
    ./RunMyprog.sh 9_9_8 -a 9 -b 9 -c 8
    ./RunMyprog.sh 9_9_9 -a 9 -b 9 -c 9

See [#DISBATCH directives](#disbatch-directives) for more ways to simplify task lines. disBatch also sets some environment variables that can be used in your commands as arguments or to generate task-specifc file names:

* `DISBATCH_JOBID`: A name disBatch creates that should be unique to the job
* `DISBATCH_NAMETASKS`: The basename of the task file
* `DISBATCH_REPEAT_INDEX`: See the repeat construct in [\#DISBATCH directives](#disbatch-directives)
* `DISBATCH_STREAM_INDEX`: The 1-based line number of the line from the task file that generated the task
" `DISBATCH_TASKID`: 0-based sequential counter value that uniquely identifies each task

Appending `_ZP` to any of the last three will produce a 0-padded value (to six places). If these variables are used to create file names, 0-padding will result in files names that sort correctly.

Once you have created the task file, running disBatch is straightforward. For example, working with a cluster managed by Slurm,
all that needs to be done is to submit a job like the following:

    sbatch -n 20 -c 4 disBatch TaskFileName

This particular invocation will allocate sufficient resources to process
20 tasks at a time, each of which needs 4 cores.
disBatch will use environment variables initialized by Slurm to determine the execution resources to use for the run.
This invocation assumes an appropriately installed disBatch is in your PATH, see [installation](#installation) for details.

disBatch also allows the pool of execution resources to be increased or decreased during the course of a run:

    sbatch -n 10 -c 4 ./TaskFileName_dbUtil.sh

will add enough resources to run 10 more tasks concurrently. `TaskFileName_dbUtl.sh` is a utility script created by `disBatch` when the run starts (the actual name is a little more complex, see [startup](#user-content-startup)).

Various log files will be created as the run unfolds:

* `TaskFileName_*_status.txt`: status of every task (details below). `*` elides a unique identifier disBatch creates to distinguish one run from another. This is the most important output file and we recommend checking it after every run.
* `TaskFileName_*_[context|driver|engine].log`:
  The disBatch driver log file contains details mostly of interest in case of a
  problem with disBatch itself. (The driver log file name can be changed with `-l`). It can generally be ignored by end
  users (but keep it around in the event that something did go
  wrong&mdash;it will aid debugging). The `*_[context|engine].log` files contain similar information for the disBatch components that manage execution resources.
* `disBatch_*_kvsinfo.txt`: TCP address of invoked KVS server if any (for additional advanced status monitoring)

> [!TIP]
> The `*_status.txt` file is the most important disBatch output file and we recommend checking it after every run.

While disBatch is a Python 3 application, it can run tasks from any language environment&mdash;anything you can run from a shell can be run as a task.

### Status file

The status file is the most important disBatch output file and we recommend checking it after every run. The filename is `TaskFileName_*_status.txt`. It contains tab-delimited lines of the form:

    314	315	-1	worker032	8016	0	10.0486528873	1458660919.78	1458660929.83	0	""	0	""	cd /path/to/workdir ; myprog -a 3 -b 1 -c 4 > task_3_1_4.log 2>&1

These fields are:

  1. Flags: The first field, blank in this case, may contain `E`, `O`, `R`, `B`, or `S` flags.
     Each program/task should be invoked in such a way that standard error
     and standard output end up in appropriate files. If that is not the case
     `E` or `O` flags will be raised. `R` indicates that the task
     returned a non-zero exit code. `B` indicates a [barrier](#disbatch-directives). `S` indicates the job was skipped (this may happen during "resume" runs).
  1. Task ID: The `314` is the 0-based index of the task (starting from the beginning of the task file, incremented for each task, including repeats).
  1. Line number: The `315` is the 1-based line from the task file. Blank lines, comments, directives and repeats may cause this to drift considerably from the value of Task ID.
  1. Repeat index: The `-1` is the repeat index (as in this example, `-1` indicates this task was not part of a repeat directive).
  1. Node: `worker032` identifies the node on which the task ran.
  1. PID: `8016` is the PID of the bash shell used to run the task.
  1. Exit code: `0` is the exit code returned.
  1. Elapsed time: `10.0486528873` (seconds),
  1. Start time:`1458660919.78` (Unix epoch based),
  1. Finish time: `1458660929.83` (Unix epoch based).
  1. Bytes of *leaked* output (not redirected to a file),
  1. Output snippet (up to 80 bytes consisting of the prefix and suffix of the output),
  1. Bytes of leaked error output,
  1. Error snippet,
  1. Command: `cd ...` is the text of the task (repeated from the task file, but subject to modification by [directives](#disbatch-directives)).


## Installation

**Users of Flatiron clusters: disBatch is available via the module system. You can run `module load disBatch` instead of installing it.**

There are several ways to get disBatch:

  1. installation with pip;
  1. direct invocation with pipx or uvx;
  1. cloning the repo.

Most users can install via pip. Direct invocation with uvx may be of particular interest for users on systems without a modern Python, as uvx will bootstrap Python for you.
  
### Installation with pip
You can use pip to install disbatch just like a normal Python package:

  1. from PyPI: `pip install disbatch`
  2. from GitHub: `pip install git+https://github.com/flatironinstitute/disBatch.git`

These should be run in a venv. Installing with `pip install --user disbatch` may work instead, but as a general practice is discouraged.

After installation, disBatch will be available via the `disbatch` and `disBatch` executables on the `PATH` so long as the venv is activated. Likewise, disBatch can be run as a module with `python -m disbatch`.

<details>
<summary>Click here for a complete example using pip and venv</summary>

You'll need a modern Python to install disBatch this way. We recommend the uvx installation method below if you don't have one, as uv will boostrap Python for you.

```
python -m venv venv
. venv/bin/activate
pip install disbatch
disbatch TaskFile
```
</details>

### Direct invocation with pipx or uvx

[pipx](https://pipx.pypa.io/stable/) and [uvx](https://docs.astral.sh/uv/guides/tools/) are two tools that will create an isolated venv, download and install disbatch into that venv, and run it all in a single command:

  1. `pipx disbatch TaskFile`
  1. `uvx disbatch TaskFile`

pipx already requires a somewhat modern Python, so for disbatch's purposes it just saves you the step of creating and activating a venv and installing disBatch.

uvx, on the other hand, will download a modern Python for you if you don't have one available locally. It requires [installing uv](https://docs.astral.sh/uv/getting-started/installation/), which is straightforward and portable.

Here's a complete example of running disbatch on a system without modern Python:

```
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env
uvx disbatch TaskFile
```

Afterwards, disbatch will always be available as `uvx disbatch`.

For Slurm users, note that the above will install disbatch into the user's default cache directory. If this directory is not visible to all nodes on the cluster, then disbatch jobs will fail. One can specify a different cache directory with `uvx --cache-dir=...`, but the simplest fix is to do a `tool install`:

```
uv tool install disbatch
sbatch disbatch TaskFile
```

This places `disbatch` on the `PATH` in a persistent location; no need to use `uvx` anymore.


### Cloning the repo
Users or developers who want to work on the code should clone the repo then do an editable install into a venv:

```
git clone https://github.com/flatironinstitute/disBatch.git
pip install -e ./disBatch
```

Setting `PYTHONPATH` may also work, but as a general practice is discouraged. If you don't have a modern Python available, [uv](https://docs.astral.sh/uv/getting-started/installation/) can bootstrap one for you.

## Execution Environments
disBatch is designed to support a variety of execution environments, from your own desktop, to a local collection of workstations, to large clusters managed by job schedulers.
It currently supports Slurm and can be executed from `sbatch`, but it is architected to make it simple to add support for other resource managers.

You can also run directly on one or more machines by setting an environment variable:

    DISBATCH_SSH_NODELIST=localhost:7,otherhost:3

or specifying an invocation argument:

    -s localhost:7,otherhost:3
    
This allows execution directly on your `localhost` and via ssh for remote hosts without the need for a resource management system.
In this example, disBatch is told it can use seven CPUs on your local host and three on `otherhost`. Assuming the default mapping of one task to one CPU applies in this example, seven tasks could be in progress at any given time on `localhost`, and three on `otherhost`. Note that `localhost` is an actual name you can use to refer to the machine on which you are currently working. `otherhost` is fictious. 
Hosts used via ssh must be set up to allow ssh to work without a password and must share the working directory for the disBatch run.

disBatch refers to a collection of execution resources as a *context* and the resources proper as *engines*. So the Slurm example `sbatch -n 20 -c 4`, run on a cluster with 16-core nodes, might create one context with five engines (one each for five 16-core nodes, capable of running four concurrent 4-core tasks each), while the SSH example creates one context with two engines (capable of running seven and three concurrent tasks, respectively).

## Invocation
```
usage: disbatch [-h] [-e] [--force-resume] [--kvsserver [HOST:PORT]]
                [--logfile FILE]
                [--loglevel {CRITICAL,ERROR,WARNING,INFO,DEBUG}] [--mailFreq N]
                [--mailTo ADDR] [-p PATH] [-r STATUSFILE] [-R] [-S]
                [--status-header] [--use-address HOST:PORT] [-w]
                [--taskcommand COMMAND] [--taskserver [HOST:PORT]]
                [-C TASK_LIMIT] [-c N] [--fill] [-g] [--no-retire] [-l COMMAND]
                [--retire-cmd COMMAND] [-s HOST:CORECOUNT] [-t N]
                [taskfile]

Use batch resources to process a file of tasks, one task per line.

positional arguments:
  taskfile              File with tasks, one task per line ("-" for stdin)

options:
  -h, --help            show this help message and exit
  -e, --exit-code       When any task fails, exit with non-zero status (default:
                        only if disBatch itself fails)
  --force-resume        With -r, proceed even if task commands/lines are
                        different.
  --kvsserver [HOST:PORT]
                        Use a running KVS server.
  --logfile FILE        Log file.
  --loglevel {CRITICAL,ERROR,WARNING,INFO,DEBUG}
                        Logging level (default: INFO).
  --mailFreq N          Send email every N task completions (default: 1). "--
                        mailTo" must be given.
  --mailTo ADDR         Mail address for task completion notification(s).
  -p PATH, --prefix PATH
                        Path for log, dbUtil, and status files (default: ".").
                        If ends with non-directory component, use as prefix for
                        these files names (default:
                        <Taskfile>_disBatch_<YYYYMMDDhhmmss>_<Random>).
  -r STATUSFILE, --resume-from STATUSFILE
                        Read the status file from a previous run and skip any
                        completed tasks (may be specified multiple times).
  -R, --retry           With -r, also retry any tasks which failed in previous
                        runs (non-zero return).
  -S, --startup-only    Startup only the disBatch server (and KVS server if
                        appropriate). Use "dbUtil..." script to add execution
                        contexts. Incompatible with "--ssh-node".
  --status-header       Add header line to status file.
  --use-address HOST:PORT
                        Specify hostname and port to use for this run.
  -w, --web             Enable web interface.
  --taskcommand COMMAND
                        Tasks will come from the command specified via the KVS
                        server (passed in the environment).
  --taskserver [HOST:PORT]
                        Tasks will come from the KVS server.
  -C TASK_LIMIT, --context-task-limit TASK_LIMIT
                        Shutdown after running COUNT tasks (0 => no limit).
  -c N, --cpusPerTask N
                        Number of cores used per task; may be fractional
                        (default: 1).
  --fill                Try to use extra cores if allocated cores exceeds
                        requested cores.
  -g, --gpu             Use assigned GPU resources [DEPRECATED]
  --no-retire           Don't retire nodes from the batch system (e.g., if
                        running as part of a larger job).
  -l COMMAND, --label COMMAND
                        Label for this context. Should be unique.
  --retire-cmd COMMAND  Shell command to run to retire a node (environment
                        includes $NODE being retired, remaining $ACTIVE node
                        list, $RETIRED node list; default based on batch
                        system). Incompatible with "--ssh-node".
  -s HOST:CORECOUNT, --ssh-node HOST:CORECOUNT
                        Run tasks over SSH on the given nodes (can be specified
                        multiple times for additional hosts; equivalent to
                        setting DISBATCH_SSH_NODELIST)
  -t N, --tasksPerNode N
                        Maximum concurrently executing tasks per node (up to
                        cores/cpusPerTask).
```

The options for mail will only work if your computing environment permits processes to access mail via SMTP.

A value for `-c` < 1 effectively allows you to run more tasks concurrently than CPUs specified for the run. This is somewhat unusual, and generally not recommended, but could be appropriate in some cases.

The `--no-retire` and `--retire-cmd` flags allow you to control what disBatch does when a node is no longer needed to run jobs.
When running under slurm, disBatch will by default run the command:

    scontrol update JobId="$SLURM_JOBID" NodeList="${DRIVER_NODE:+$DRIVER_NODE,}$ACTIVE"

which will tell slurm to release any nodes no longer being used.
You can set this to run a different command, or nothing at all.
While running this command, the follow environment variables will be set: `NODE` (the node that is no longer needed), `ACTIVE` (a comma-delimited list of nodes that are still active), `RETIRED` (a comma-delimited list of nodes that are no longer active, including `$NODE`), and possibly `DRIVER_NODE` (the node still running the main disBatch script, if it's not in `ACTIVE`).

`-S` Startup only mode. In this mode, `disBatch` starts up the task management system and then waits for execution resources to be added.
<span id='user-content-startup'>At startup</span>, `disBatch` always generates a script `<Prefix>_dbUtil.sh`, where `<Prefix>` refers to the `-p` option or default, see above. We'll call this simply `dbUtils.sh` here,
but remember to include `<Prefix>_` in actual use. You can add execution resources by doing one or more of the following multiple times:
1. Submit `dbUtils.sh` as a job, e.g.:

    `sbatch -n 40 dbUtil.sh`

2. Use ssh, e.g.:

    `./dbUtil.sh -s localhost:4,friendlyNeighbor:5`

Each of these creates an execution context, which contains one of more execution engines (if using, for example, 8-core nodes, then five for the first; two in the second).
An engine can run one or more tasks currently. In the first example, each of the five engines will run up to eight tasks concurrently, while in the
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

`--use-address HOST:PORT` can be used if disBatch is not able to determine the correct hostname for the machine it is running on (or you need to override what was detected). This is often the case when running on a personal laptop without a "real" network configuration. In this case `--use-address=localhost:0` will generally be sufficient.

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

## #DISBATCH directives

### PREFIX and SUFFIX

In order to simplify task files, disBatch supports a couple of
directives to specify common task prefix strings and suffix strings. As noted above, it
also sets environment variables to identify various aspects of the
submission. Here's an example

    # Note there is a space at the end of the next line.
    #DISBATCH PREFIX ( cd /path/to/workdir ; source SetupEnv ; 
    #DISBATCH SUFFIX  ) &> ${DISBATCH_NAMETASKS}_${DISBATCH_JOBID}_${DISBATCH_TASKID_ZP}.log

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

    #DISBATCH REPEAT 5 myprog file${DISBATCH_REPEAT_INDEX}

will expand into five tasks, each with the environment variable
`DISBATCH_REPEAT_INDEX` set to one of 0, 1, 2, 3 or 4.

The starting index and step size can also be changed:

    #DISBATCH REPEAT 5 start 100 step 50 myprog file${DISBATCH_REPEAT_INDEX}

This will result in indices 100, 150, 200, 250, and 300. `start` defaults
to 0, and `step` to 1.

The command is actually optional; one might want to omit the command
if a prefix and/or suffix are in place. Returning to our earlier example, the task file
could be:

    #DISBATCH PREFIX a=$((DISBATCH_REPEAT_INDEX/100)) b=$(((DISBATCH_REPEAT_INDEX%100)/10 )) c=$((DISBATCH_REPEAT_INDEX%10) ; ( cd /path/to/workdir ; source SetupEnv ; myprog -a $a -b $b -c $c ) &> task_${a}_${b}_${c}.log
    #DISBATCH REPEAT 1000

This is not a model of clarity, but it does illustrate that the repeat constuct can be relatively powerful. Many users may find it more convenient to use the tool of their choice to generate a text file with 1000 invocations explictly written out.

### PERENGINE

    #DISBATCH PERENGINE START { command ; sequence ; } &> engine_start_${DISBATCH_ENGINE_RANK}.log
    #DISBATCH PERENGINE STOP { command ; sequence ; } &> engine_stop_${DISBATCH_ENGINE_RANK}.log

Use these to specify commands that should run at the time an engine joins a disBatch run or at the time the engine leaves the disBatch run, respectively.
You could, for example, use these to bulk copy some heavily referenced read-only data to the engine's local storage area before any tasks are run, and then delete that data when the engine shuts down.
You can use the environment variable DISBATCH_ENGINE_RANK to distinguish one engine from another; for example, it is used here to keep log files separate.

These directives must come before any other tasks.

## Embedded disBatch

You can start disBatch from within a python script by instantiating a "DisBatcher" object.

See `exampleTaskFiles/dberTest.py` for an example.

The "DisBatcher" class (defined in `disbatch/disBatch.py`) illustrates how to interact with disBatch via KVS. This approach could be used to enable similar functionality in other language settings.

## License

Copyright 2024 Simons Foundation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
