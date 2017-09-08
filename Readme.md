Distributed processing of a batch of tasks.
===========================================

## Overview

One common usage pattern for distributed computing involves processing a
long list of commands (aka *tasks*):

    cd /path/to/workdir ; myprog argsFor000 > task000.log 2>&1
    cd /path/to/workdir ; myprog argsFor001 > task001.log 2>&1
     ... 
    cd /path/to/workdir ; myprog argsFor998 > task998.log 2>&1
    cd /path/to/workdir ; myprog argsFor999 > task999.log 2>&1

One could do this by submitting 1,000 separate jobs to a cluster computer, but that may
present problems for the queuing system and can behave badly if the
system is configured to handle jobs in a simple first come, first serve
fashion.

Another alternative is to use a resource management system's native
support for job arrays, but this often requires massaging the commands
to reflect syntax specific to a particular system's implementation of
job arrays.

And what if you don't have a cluster available, but do have a collection of networked computers?

In any event, when processing such a list of tasks, it is helpful to
acquire metadata about the execution of each task: where it ran, how
long it took, its exit return code, etc.

**disBatch.py** has been designed to support this usage in a simple and
portable way, as well as to provide the sort of metadata that can be
helpful for debugging and reissuing failed tasks.

It can take as input a file, each of whose lines is a task in the form of a
command sequence (as in the above example). It launches the tasks one
after the other until all specified execution resources are in use. Then as one
executing task exits, the next task in the file is launched until all
the lines in the file have been processed.

In the simplest case, working with a cluster managed by SLURM, all that needs to be done is to write the command
sequences to a file and then submit a job like the following:

    sbatch -n 20 --ntasks-per-node 5 --exclusive --wrap "disBatch.py TaskFileName"

This particular invocation will allocate sufficient resources to process
20 tasks at a time, with no more than five running concurrently on any
given node. Every node allocated will be running only tasks associated
with this submission. disBatch will use environment variables initialized by SLURM to determine the execution resources to use for the run.

Various log files will be created as the run unfolds:

* `TaskFileName_134504_status.txt` (assuming the SLURM Job ID is `134504`): status of every task (details below)
* `disBatch_134504_driver.txt` (can be changed with `-l`), `TaskFileName_134504_worker032_engine.txt`: 
  The disBatch log file contains details mostly of interest in case of a
  problem with disBatch itself. It can generally be ignored by end
  users (but keep it around in the event that something did go
  wrong---it will aid debugging). The ``*_engine.txt'' files contain similar information for each node acting as an execution resource.

## Installation

`disBatch.py` requires the `kvsstcp` package, which should be installed in python's path, or placed in this directory.
You can simply clone this git repository with `--recursive` (or run `git submodule update --init` if you've already cloned it).

disBatch is designed to support a variety of execution environments, from a local collection of workstations to large clusters managed by job schedulers.
It currently supports SLURM and can be executed from `sbatch`, but it is architected to make it simple to add support for other resource managers.

You can also run directly on one or more machines over ssh by setting an environment variable:

         DISBATCH_SSH_NODELIST=host1:7,host2:3

This allows execution via ssh (or directly on `localhost`) without the need for a resource management system.
In this example, disBatch is told it can use seven CPUs on host1 and three on host2. Assuming the default mapping of one task to one CPU applies in this example, seven tasks could be in progress at any given time on host1, and three on host2.
Hosts used via ssh must be set up to allow ssh to work without a password.

Depending on your execution environment, the ability of disBatch to determine the location of itself and kvsstcp may be disrupted. If you get errors about not finding disBatch or kvsstcp, you may need to hard-code the paths for your setup into the script.
You can do this automatically by running `./disBatch.py --fix-paths`. This should only need to be done once.


## Invocation
~~~~
  -h, --help            show this help message and exit
  --fix-paths           Configure fixed path to script and modules.
  -l LOGFILE, --logfile LOGFILE
                        Log file.
  --mailFreq N          Send email every N task completions (default: 1). "--mailTo" must be given.
  --mailTo MAILTO       Mail address for task completion notification(s).
  -c CPUSPERTASK, --cpusPerTask CPUSPERTASK
                        Number of cores used per task; may be fractional (default: 1).
  -t TASKSPERNODE, --tasksPerNode TASKSPERNODE
                        Maximum concurrently executing tasks per node (up to cores/cpusPerTask).
  -r STATUSFILE, --resume-from STATUSFILE
                        Read the status file from a previous run and skip any completed tasks (may be specified multiple times).
  -R, --retry           With -r, also retry any tasks which failed in previous runs (non-zero return).
  --force-resume        With -r, proceed even if task commands/lines are different.
  -w, --web             Enable web interface.
  --kvsserver [HOST:PORT]
                        Use a running KVS server.
  --taskcommand COMMAND
                        Tasks will come from the command specified via the KVS server (passed in the environment).
  --taskserver [HOST:PORT]
                        Tasks will come from the KVS server.
~~~~

The options for mail will only work if you computing environment permits processes to access mail via SMTP.

A value for `-c` < 1 effectively allows you to run more tasks concurrently than CPUs specified for the run. This is somewhat unusual, and generally not recommended, but could be appropriate in some cases.

`-r` uses the status file of a previous run to determine what tasks to run during this disBatch invocation. Only those tasks that haven't yet run (or with `-R`, those that haven't run or did but returned a non-0 exit code) are run this time. By default, the numeric task identifier and the text of the command are used to determine if a current task is the same as one found in the status file. `--force-resume` restricts the comparison to just the numeric identifier.

`--kvsserver`, `--taskcommand`, and `--taskserver` implement advance functionality (placing disBatch in an existing shared key store context and allowing for a programmatic rather than textual task interface). Contact the authors for more details.


### Status file

The `_status.txt` files contains tab-delimited lines of the form:

     	314	315	-1	worker032	8016	0	10.0486528873	1458660919.78	1458660929.83	0	""	0	""	'cd /path/to/workdir ; myprog argsFor314 > task314.log 2>&1'

These fields are:

  1. Flags: The first field, blank in this case, may contain `E`, `O`, `R`, `B`, or `S` flags.
     Each program/task should be invoked in such a way that standard error
     and standard output end up in appropriate files. If that's not the case
     `E` or `O` flags will be raised. `R` indicates that the task
     returned a non-zero exit code. `B` indicates a barrier (see below). `S` indicates the job was skip (this may happen during "resume" runs).
  1. Task ID: The `314` is the 0-based index of the task (starting from the beginning of the task file, incremented for each task, including repeats).
  1. Line number: The `315` is the 1-based line from the task file. Blank lines, directives and comments may cause this to drift considerably from the value of Task ID.
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
    #DISBATCH PREFIX cd /path/to/workdir ; 
    #DISBATCH SUFFIX > ${DISBATCH_NAMETASKS}_${DISBATCH_JOBID}_${DISBATCH_TASKID}.log 2>&1

These are textually prepended and appended, respectively, to the text of
each task line. If the suffix includes redirection and a task is a proper command sequence (a series of
program invocations joined by `;`), then the task should be wrapped in `( ...)` so that the standard error and standard output of the whole sequence
will be redirected to the log file. If this is not done, only standard
error and standard output for the last component of the command sequence
will be captured. This is probably not what you want unless you have
redirected these outputs for the previous individual parts of the
command sequence.

Using these, the above commands could be replaced with:

    myprog argsFor000
    myprog argsFor001
     ... 
    myprog argsFor998
    myprog argsFor999

Note: the log files will have somewhat different, but still task
specific, names.

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
until all tasks in progress have completed. Advanced feature: if `BARRIER` is followed by a string, that string is interpreted as a key name. When the barrier completes, the Task ID of the barrier will be `put` to that key.

### REPEAT

For those problems that are easily handled via a job-array-like approach:

         #DISBATCH REPEAT 5 start 100 step 50 [command]

will expand into five tasks, each with the environment variable
`DISBATCH_REPEAT_INDEX` set to one of 100, 150, 200, 250, or 300.
The tasks will consists of the concatenation of the prefix, command (if provided),
and the suffix currently in effect. `start` defaults to 0, `step`
to 1. Note: the semantics here differ somewhat from many range
constructs, the number immediately following `REPEAT` sets the
number of tasks that will be executed; the next two numbers affect
only the value that the repeat index will have in
the environment for each of the repeat task instances. So, returning to our earlier example, the task file
could be:

        #DISBATCH PREFIX cd /path/to/workdir ; myprog argsFor$(printf "%03d" ${DISBATCH_REPEAT_INDEX}) > ${DISBATCH_NAMETASKS}_${DISBATCH_JOBID}_${DISBATCH_TASKID}.log 2>&1
        #DISBATCH REPEAT 1000

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
