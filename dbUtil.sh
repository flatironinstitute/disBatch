#!/bin/bash

DISBATCH_ROOT={dbRoot:s}
KVSSTCP_HOST={kvsserver:s}

if [[ $1 == '--mon' ]]
then
    ${{DISBATCH_ROOT}}/dbMon.py ${{KVSSTCP_HOST}} {uniqueId:s}
else
    ${{DISBATCH_ROOT}}/disBatch.py --context ${{KVSSTCP_HOST}} "$@" < /dev/null &> {uniqueId:s}_${{BASHPID}}_context_launch.log
fi
