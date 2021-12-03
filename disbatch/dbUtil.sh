#!/bin/bash

export DISBATCH_KVSSTCP_HOST={kvsserver:s} DISBATCH_BIN={DbBin:s}

if [[ $1 == '--mon' ]]
then
    exec ${{DISBATCH_BIN}}/dbMon.py {uniqueId:s}
elif [[ $1 == '--engine' ]]
then
    exec ${{DISBATCH_BIN}}/disBatch.py "$@"
else
    exec ${{DISBATCH_BIN}}/disBatch.py --context {DbUtilPath:} "$@" < /dev/null &> {uniqueId:s}_${{BASHPID}}_context_launch.log
fi
