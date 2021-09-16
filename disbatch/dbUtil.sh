#!/bin/bash

export DISBATCH_KVSSTCP_HOST={kvsserver:s} DISBATCH_ROOT={DbRoot:s}

if [[ $1 == '--mon' ]]
then
    exec ${{DISBATCH_ROOT}}/dbMon.py {uniqueId:s}
elif [[ $1 == '--engine' ]]
then
    exec ${{DISBATCH_ROOT}}/disBatch.py "$@"
else
    exec ${{DISBATCH_ROOT}}/disBatch.py --context {DbUtilPath:} "$@" < /dev/null &> {uniqueId:s}_${{BASHPID}}_context_launch.log
fi
