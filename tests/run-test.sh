#!/bin/bash

SCENARIO=$1
BACKEND=$2

if ["$BACKEND" = "abt-io"]; then
    STORAGE=`mktemp -d test-log-XXXXXXXX`
    STORAGE="-p $STORAGE"
else
    STORAGE=""
fi

timeout 600 ./mraft-test na+sm -n 3 -f $SCENARIO $STORAGE -l $BACKEND
RET=$?

if [ $RET -eq 0 ]
then
    if [[ -n $STORAGE ]]; then
        rm -rf $STORAGE
    fi
fi

exit $RET
