#!/bin/bash

exit_fail() {
    err=$?
    echo "Slurm test failed! Output is in $workdir"
    exit $err
}

trap exit_fail ERR

workdir=$(mktemp -d -p $PWD disbatch-test.XXXX)
cp Tasks Tasks_failfast $workdir
cd $workdir

# Run the test
salloc -n 2 disBatch Tasks

# Check that all 3 tasks ran,
# which means A.txt, B.txt, and C.txt exist
[[ -f A.txt && -f B.txt && -f C.txt ]]
rm -f A.txt B.txt C.txt

# Add a task and check that we can resume
echo "touch D.txt" >> Tasks
salloc -n 2 disBatch Tasks -r Tasks*_status.txt

[[ -f D.txt && ! -f A.txt && ! -f B.txt && ! -f C.txt ]]

# Test empty task file
salloc -n 2 disBatch /dev/null

# disBatch is expected to exit with a non-zero exit code here
salloc -n 2 disBatch --fail-fast Tasks_failfast || true

# check that we failed fast and didn't run any more tasks
[[ ! -f A.txt ]]

cd - > /dev/null

trap - ERR
echo "Slurm test passed."
# NFS sometimes leaves stale file handles, but don't fail the test
rm -rf $workdir || true
