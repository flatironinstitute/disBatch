#!/bin/bash

set -e

workdir=$(mktemp -d -p ./ disbatch-test.XXXX)
cp Tasks $workdir
cd $workdir

# Run the test
salloc -n 2 disBatch Tasks

# Check that all 3 tasks ran,
# which means A.txt, B.txt, and C.txt exist
success=0
[[ -f A.txt && -f B.txt && -f C.txt ]] || success=$?

cd - > /dev/null

if [[ $success -eq 0 ]]; then
    echo "Slurm test passed."
    rm -rf $workdir
else
    echo "Slurm test failed! Output is in $workdir"
fi

exit $success
