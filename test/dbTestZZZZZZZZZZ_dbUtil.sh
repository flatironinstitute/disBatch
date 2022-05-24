#!/bin/bash

export DISBATCH_KVSSTCP_HOST=10.128.45.55:33137 DISBATCH_ROOT=/cm/shared/sw/pkg/flatiron/disBatch/2.0-beta

if [[ $1 == '--mon' ]]
then
    exec ${DISBATCH_ROOT}/dbMon.py /mnt/home/yliu/projects/disBatch/dbTestZZZZZZZZZZ
elif [[ $1 == '--engine' ]]
then
    exec ${DISBATCH_ROOT}/disBatch.py "$@"
else
    exec ${DISBATCH_ROOT}/disBatch.py --context /mnt/home/yliu/projects/disBatch/dbTestZZZZZZZZZZ_dbUtil.sh "$@" < /dev/null &> /mnt/home/yliu/projects/disBatch/dbTestZZZZZZZZZZ_${BASHPID}_context_launch.log
fi
