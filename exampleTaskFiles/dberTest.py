#!/bin/env python3

import sys
from disbatch import disBatch

# This test script requires at least one argument: the number of tasks to run.
# The rest, if any, are arguments that will be passed to disBatch:
#
#  - If testing on your local machine, try something like
#
#      dberTest.py 15 -s localhost:5
#
#  - If testing via a SLURM submission, disBatch will auto detect
#    that, so no additional arguments are needed
#
NumTasks = int(sys.argv[1])
dbArgs = sys.argv[2:]

# The first argument is a prefix that will be used internally to
# identify support activities related to this run. The rest are
# arguments for disBatch.
db = disBatch.DisBatcher(tasksname='testing', args=dbArgs)

# We use this to keep track of the tasks.
# disBatch assigns a numeric ID to each tasks, starting from 0. We need
# to do the same to track the tasks.
tasks = {}
for x in range(NumTasks):
    # Tasks are simply ASCII command lines. The '{}' in the following
    # are interpreted by python, not bash.
    # We force an error return of task 7.
    tasks[x] = f'{{ date ; hostname ; sleep 2 ; echo {x}^2 $(( {x} * {x} )) ; [[ {x} == 7 ]] && exit 1 ; date ; }} > square.log_{x:03d} 2>&1 '

    # Submit the task.
    db.submit(tasks[x])

# syncTasks waits for all tasks identified by the keys of "tasks" to
# complete. It returns a dictionary that maps an id to a return code
# and the complete status report for the task.  syncTasks maintains an
# internal dictionary of return codes, so this operation is
# idempotent.
tid2status = db.syncTasks(tasks)
for tid in tasks:
    print('task %d: %s returned %d, matched: %s'%(tid, repr(tasks[tid]), tid2status[tid]['ReturnCode'], repr(tasks[tid]) == tid2status[tid]['TaskCmd']))

# Now try a repeat construct. Force an error for the index 112.
db.submit(f'#DISBATCH REPEAT {NumTasks} start 100 step 3 x=${{DISBATCH_REPEAT_INDEX}} ; {{ date ; hostname ; sleep 2 ; echo $x^3 $(( x * x * x )) ; [[ $x == 112 ]] && exit 1 ; date ; }} > cube.log_$(printf "%03d" $x) 2>&1')

# The ids for the new tasks are the next NumTasks consecutive integers.
target_tids = set(range(NumTasks, 2*NumTasks))
for x in range(NumTasks):
    # Wait for one task and return its status info.
    s = db.wait_one_task()
    assert s['TaskId'] in target_tids
    print('task %d: returned %d, "%s"'%(s['TaskId'], s['ReturnCode'], s['TaskCmd']))

# Tell DisBatcher no more tasks are coming.
db.done()
