#!/bin/bash

export DISBATCH_KVSSTCP_HOST=10.128.45.55:36334 DISBATCH_ROOT=/cm/shared/sw/pkg/flatiron/disBatch/2.0-beta

if [[ $1 == '--mon' ]]
then
    exec ${DISBATCH_ROOT}/dbMon.py /mnt/home/yliu/projects/disBatch/tmp/dbTestZZZZZZZZZZ
elif [[ $1 == '--engine' ]]
then
    exec ${DISBATCH_ROOT}/disBatch.py "$@"
else
    exec ${DISBATCH_ROOT}/disBatch.py --context /mnt/home/yliu/projects/disBatch/tmp/dbTestZZZZZZZZZZ_dbUtil.sh "$@" < /dev/null &> /mnt/home/yliu/projects/disBatch/tmp/dbTestZZZZZZZZZZ_${BASHPID}_context_launch.log
fi
