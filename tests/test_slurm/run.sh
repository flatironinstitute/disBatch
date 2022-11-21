#!/usr/bin/bash

workdir=$(mktemp -d -p ./ disbatch-test.XXXX)
cp Tasks $workdir
cd $workdir

# Run the test
salloc -n 2 disBatch Tasks

# Check that all 3 tasks ran,
# which means A.txt, B.txt, and C.txt exist
[[ -f A.txt && -f B.txt && -f C.txt ]]
success=$?

if [[ $success -eq 0 ]]; then
    echo "Slurm test passed."
else
    echo "Slurm test failed!"
fi

exit $success
