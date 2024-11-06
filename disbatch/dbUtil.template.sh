#!/bin/bash

export DISBATCH_KVSSTCP_HOST={kvsserver:s}

if [[ $1 == '--mon' ]]
then
    exec {DisBatchPython} -m disbatch.dbMon {uniqueId:s}
elif [[ $1 == '--engine' ]]
then
    exec {DisBatchPython} -m disbatch "$@"
else
    exec {DisBatchPython} -m disbatch --context {DbUtilPath:} "$@" < /dev/null 1> {uniqueId:s}_${{BASHPID-$$}}_context_launch.log
fi
