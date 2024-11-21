#!/bin/bash

exit_fail() {
    err=$?
    echo "SSH test failed! Output is in $workdir"
    exit $err
}

trap exit_fail ERR

workdir=$(mktemp -d -p $PWD disbatch-test.XXXX)
cp Tasks Tasks_failfast $workdir
cd $workdir

# Run the test
disBatch -s localhost:2 Tasks

# Check that all 3 tasks ran,
# which means A.txt, B.txt, and C.txt exist
[[ -f A.txt && -f B.txt && -f C.txt ]]
rm -f A.txt B.txt C.txt

# Add a task and check that we can resume
echo "touch D.txt" >> Tasks
disBatch -s localhost:2 Tasks -r Tasks*_status.txt

[[ -f D.txt && ! -f A.txt && ! -f B.txt && ! -f C.txt ]]

# Test empty task file
disBatch -s localhost:2 /dev/null

# disBatch is expected to exit with a non-zero exit code here
disbatch -s localhost:2 --fail-fast Tasks_failfast || true

# check that we failed fast and didn't run any more tasks
[[ ! -f A.txt ]]

cd - > /dev/null

trap - ERR
echo "SSH test passed."
rm -rf $workdir
