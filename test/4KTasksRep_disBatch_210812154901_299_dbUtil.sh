#!/bin/bash

export DISBATCH_KVSSTCP_HOST=10.128.45.55:40516 DISBATCH_ROOT=/cm/shared/sw/pkg/flatiron/disBatch/2.0-beta

if [[ $1 == '--mon' ]]
then
    exec ${DISBATCH_ROOT}/dbMon.py /mnt/home/yliu/projects/disBatch/4KTasksRep_disBatch_210812154901_299
elif [[ $1 == '--engine' ]]
then
    exec ${DISBATCH_ROOT}/disBatch.py "$@"
else
    exec ${DISBATCH_ROOT}/disBatch.py --context /mnt/home/yliu/projects/disBatch/4KTasksRep_disBatch_210812154901_299_dbUtil.sh "$@" < /dev/null &> /mnt/home/yliu/projects/disBatch/4KTasksRep_disBatch_210812154901_299_${BASHPID}_context_launch.log
fi
