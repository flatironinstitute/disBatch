#!/bin/bash

set -e

workdir=$(mktemp -d -p ./ disbatch-test.XXXX)
cp Tasks Tasks_failfast $workdir
cd $workdir

# Run the test
disBatch -s localhost:2 Tasks

# Check that all 3 tasks ran,
# which means A.txt, B.txt, and C.txt exist
[[ -f A.txt && -f B.txt && -f C.txt ]]
success=$?

rm A.txt B.txt C.txt
disbatch -s localhost:2 --fail-fast Tasks_failfast
[[ ! -f A.txt ]]
success=$((success + $?))

cd - > /dev/null

if [[ $success -eq 0 ]]; then
    echo "SSH test passed."
    rm -rf $workdir
else
    echo "SSH test failed! Output is in $workdir"
fi

exit $success
