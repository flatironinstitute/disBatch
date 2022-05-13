#!/bin/bash

export DISBATCH_KVSSTCP_HOST={kvsserver:s} PYTHONPATH={DisBatchRoot:s}:${{PYTHONPATH}}

if [[ $1 == '--mon' ]]
then
    exec {DisBatchPython} {DisBatchRoot:s}/disbatch/dbMon.py {uniqueId:s}
elif [[ $1 == '--engine' ]]
then
    exec {DisBatchPython} -c 'from disbatch import disBatch ; disBatch.main()' "$@"
else
    exec {DisBatchPython} -c 'from disbatch import disBatch ; disBatch.main()' --context {DbUtilPath:} "$@" < /dev/null &> {uniqueId:s}_${{BASHPID}}_context_launch.log
fi
