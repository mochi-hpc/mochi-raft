#!/bin/bash

SCENARIO=$1
STORAGE=`mktemp -d test-log-XXXXXXXX`

timeout 600 ./mraft-test na+sm -n 3 -f $1 -p $STORAGE
RET=$?

if [ $RET -eq 0 ]
then
    rm -rf $STORAGE
fi

exit $RET
