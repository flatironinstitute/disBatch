#!/bin/bash

export DISBATCH_KVSSTCP_HOST=10.128.46.21:34707 DISBATCH_ROOT=/mnt/home/yliu/projects/disBatch/2.0-beta/

if [[ $1 == '--mon' ]]
then
    exec ${DISBATCH_ROOT}/dbMon.py /mnt/home/yliu/projects/disBatch/tmp/dbTest
elif [[ $1 == '--engine' ]]
then
    exec ${DISBATCH_ROOT}/disBatch.py "$@"
else
    exec ${DISBATCH_ROOT}/disBatch.py --context /mnt/home/yliu/projects/disBatch/tmp/dbTest_dbUtil.sh "$@" < /dev/null &> /mnt/home/yliu/projects/disBatch/tmp/dbTest_${BASHPID}_context_launch.log
fi

