#!/bin/bash

SCENARIO=$1
BACKEND=$2

if [ "$BACKEND" = "abt-io" ]; then
    STORAGE=`mktemp -d test-log-XXXXXXXX`
    P_STORAGE="-p $STORAGE"
else
    P_STORAGE=""
fi

timeout 600 ./mraft-test na+sm -n 3 -f $SCENARIO $P_STORAGE -l $BACKEND
RET=$?

if [ $RET -eq 0 ]
then
    if [[ -n $P_STORAGE ]]; then
        rm -rf $STORAGE
    fi
fi

exit $RET
