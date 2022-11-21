#!/usr/bin/bash

workdir=$(mktemp -d -p ./ disbatch-test.XXXX)
cp Tasks $workdir
cd $workdir

# Run the test
disBatch -s localhost:2 Tasks

# Check that all 3 tasks ran,
# which means A.txt, B.txt, and C.txt exist
[[ -f A.txt && -f B.txt && -f C.txt ]]
success=$?

if [[ $success -eq 0 ]]; then
    echo "SSH test passed."
else
    echo "SSH test failed!"
fi

exit $success
