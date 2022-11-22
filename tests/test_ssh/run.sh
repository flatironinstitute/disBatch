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

cd -

if [[ $success -eq 0 ]]; then
    echo "SSH test passed."
    rm -rf $workdir
else
    echo "SSH test failed! Output is in $workdir"
fi

exit $success
