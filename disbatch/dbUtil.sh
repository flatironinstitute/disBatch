#!/bin/bash

export DISBATCH_KVSSTCP_HOST={kvsserver:s} DISBATCH_ROOT={DbRoot:s}/..
DISBATCH_BIN=${{DISBATCH_ROOT}}

# This should find the correct path to disBatch for pip installs
if [ ! -f "$DISBATCH_ROOT/disBatch.py" ]
then
    export DISBATCH_ROOT="${{DISBATCH_ROOT}}/../../../"
    DISBATCH_BIN=${{DISBATCH_ROOT}}/bin
fi

if [[ $1 == '--mon' ]]
then
    exec ${{DISBATCH_BIN}}/dbMon.py {uniqueId:s}
elif [[ $1 == '--engine' ]]
then
    exec ${{DISBATCH_BIN}}/disBatch.py "$@"
else
    exec ${{DISBATCH_BIN}}/disBatch.py --context {DbUtilPath:} "$@" < /dev/null &> {uniqueId:s}_${{BASHPID}}_context_launch.log
fi
