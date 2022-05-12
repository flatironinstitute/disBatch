#!/bin/bash

export DISBATCH_KVSSTCP_HOST={kvsserver:s} DISBATCH_ROOT={DisBatchRoot:s}

if [[ $1 == '--mon' ]]
then
    exec {DisBatchPython} ${{DISBATCH_ROOT}}/disbatch/dbMon.py {uniqueId:s}
elif [[ $1 == '--engine' ]]
then
    exec {DisBatchPython} ${{DISBATCH_ROOT}}/disbatch/disBatch_cli.py "$@"
else
    exec {DisBatchPython} ${{DISBATCH_ROOT}}/disbatch/disBatch_cli.py --context {DbUtilPath:} "$@" < /dev/null &> {uniqueId:s}_${{BASHPID}}_context_launch.log
fi
