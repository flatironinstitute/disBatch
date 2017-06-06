Distributed processing of a batch of tasks.
===========================================

One common usage pattern for cluster computing involves processing a
long list of commands (aka *tasks*):

    cd /path/to/workdir ; myprog argsFor000 > task000.log 2>&1
    cd /path/to/workdir ; myprog argsFor001 > task001.log 2>&1
    ... 
    cd /path/to/workdir ; myprog argsFor998 > task998.log 2>&1
    cd /path/to/workdir ; myprog argsFor999 > task999.log 2>&1

One could do this by submitting 1,000 separate jobs, but that may
present problems for the queuing system and can behave badly if the
system is configured to handle jobs in a simple first come, first serve
fashion.

Another alternative is to use the resource management system's native
support for job arrays, but this often requires massaging the commands
to reflect syntax specific to a particular system's implementation of
job arrays.

In any event, when processing such a list of tasks, it is helpful to
acquire metadata about the execution of each task: where it ran, how
long it took, its exit return code, etc.

**disBatch.py** has been designed to support this usage in a simple and
portable way, as well as to provide the sort of metadata that can be
helpful for debugging and reissuing failed tasks.

It takes as input a file, each of whose lines is a task in the form of a
command sequence (as in the above example). It launches the tasks one
after the other until all allocated resources are in use. Then as one
executing task exits, the next task in the file is launched until all
the lines in the file have been processed.

In the simplest case, all that needs to be done is to write the command
sequences to a file and then submit a job like the following:

    sbatch -n 20 --ntasks-per-node 5 --exclusive /mnt/xfs1/home/carriero/projects/parBatch/parSlurm/disBatch.py TaskFileName

This particular invocation will allocate sufficient resources to process
20 tasks at a time, with no more than five running concurrently on any
given node. Every node allocated will be running only tasks associated
with this submission.

Four files will be created as the run unfolds:

    -rw-r--r-- 1 carriero carriero 811   Mar 22 11:35 TaskFileName_134504_dbwrapper_log.txt
    -rw-r--r-- 1 carriero carriero 201   Mar 22 11:35 TaskFileName_134504_failed.txt
    -rw-r--r-- 1 carriero carriero 21591 Mar 22 11:35 TaskFileName_134504_log.txt
    -rw-r--r-- 1 carriero carriero 8167  Mar 22 11:35 TaskFileName_134504_status.txt

The `_status.txt` files contains lines of the form:

    314 314 worker032 8016 0 10.0486528873 1458660919.78 1458660929.83 0 0 'cd /path/to/workdir ; myprog argsFor314 > task314.log 2>&1'

The first field, blank here, may contain `E`, `O` and `R` flags.
Each program/task should be invoked in such a way that standard error
and standard output end up in appropriate files. If that's not the case
`E` or `O` flags will be raised. `R` indicates that the task
returned a non-zero exit code.

The first `314` is the 0-based index of the task (starting from the
beginning of the task file). The second is the 0-based line number.
These two numbers may differ if blank lines or comments are present in
the task file.

`worker032` identifies the node on which the task ran, `8016` is the
PID of the bash shell used to run the task, `0` is the exit code
returned.

`10.0486528873 1458660919.78 1458660929.83` are the elapsed time,
start time (since the epoch), and finish time.

`0 0` reports the number of bytes of standard output and standard
error that *leaked*, i.e. were not redirected to a file.

`cd ...` is the text of the task (repeated from the task file, but
see below).

The `_failed.txt` file contains commands that failed in a format
suitable for using with another disBatch invocation. Of course, it is a
very good idea to determine the cause of failures before resubmitting
them.

The disBatch log files contain details mostly of interest in case of a
problem with disBatch itself. These can generally be ignored by end
users (but keep them around in the event that something did go
wrong---they will aid debugging).

If you do submit jobs with order 10000 or more tasks, you should
carefully consider how you want to organize the output (and error) files
produced by each of the tasks. It is generally a bad idea to have more
than a few thousand files in any one directory, so you will probably
want to introduce at least one extra level of directory hierarchy so
that the files can be divided into smaller groups. Intermediate
directory `13`, say, might hold all the files for tasks 13000 to
13999.

#### \#DISBATCH directives

In order to simplify task files, disBatch supports a couple of
directives to specify common task prefix strings and suffix strings. It
also provides environment variables to identify various aspects of the
submission. Here's an example

    # Note there is a space at the end of the next line.
    #DISBATCH PREFIX cd /path/to/workdir ; 
    #DISBATCH SUFFIX > ${DISBATCH_NAMETASKS}_${DISBATCH_JOBID}_${DISBATCH_TASKID}.log 2>&1

These are textually prepended and appended, respectively, to the text of
each task line. If a task is a proper command sequence (a series of
program invocations joined by `;`), it should be wrapped in `( ...
)` so that the standard error and standard output of the whole sequece
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
reported in the status and failed files include the prefix and suffix in
force at the time the task was launched.

------------------------------------------------------------------------

If your tasks fall into groups where a later group should only begin
after all tasks of the previous group have completely finished, you can
use this directive:

    #DISBATCH BARRIER

When disBatch encounters this directive, it will not launch another task
until all tasks in progress have completed.

#### Beta Version

The latest development version incorporates the following changes:

-   Redesigned internals to support runs with 1000s of cores.

-   Setting an environment variable:

         DISBATCH_SSH_NODELIST=host1:7,host2:3 

    allows execution via ssh (or directly on `localhost`) without the
    need for a resource management system. In this example, seven tasks
    could be in progress at any given time on host1, and three on host2.
    Hosts used via ssh must be set up to allow ssh to work without a
    password.

-   For those problems that are easily handled via a job-array-like
    approach:
     
         #DISBATCH REPEAT 5 start 100 step 50 [command]

    will expand into five tasks, each with the environment variable
    `DISBATCH_REPEAT_INDEX` set to one of 100, 150, 200, 250, or 300.
    The tasks will consists simply of the concatenation of the prefix
    and suffix currently in effect. `start` defaults to 0, `step`
    to 1. Note: the semantics here differ somewhat from many range
    constructs; the number immediately following `REPEAT` sets the
    number of tasks that will be executed, the next two numbers affect
    only the value of the repeat index each task will have in its
    environment. So, returning to our earlier example, the task file
    could be:

        #DISBATCH PREFIX cd /path/to/workdir ; myprog argsFor$(printf "%03d" ${DISBATCH_REPEAT_INDEX}) > ${DISBATCH_NAMETASKS}_${DISBATCH_JOBID}_${DISBATCH_TASKID}.log 2>&1
        #DISBATCH REPEAT 1000
