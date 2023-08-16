#!/bin/bash

export DISBATCH_KVSSTCP_HOST={kvsserver:s} PYTHONPATH={DisBatchRoot:s}:${{PYTHONPATH}}

if [[ $1 == '--mon' ]]
then
    exec {DisBatchPython} {DisBatchRoot:s}/disbatchc/dbMon.py {uniqueId:s}
elif [[ $1 == '--engine' ]]
then
    exec {DisBatchPython} {DisBatchRoot:s}/disBatch "$@"
else
    exec {DisBatchPython} {DisBatchRoot:s}/disBatch --context {DbUtilPath:} "$@" < /dev/null 1> {uniqueId:s}_${{BASHPID-$$}}_context_launch.log
fi
