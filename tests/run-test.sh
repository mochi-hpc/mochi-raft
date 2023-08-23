#!/bin/bash

SCENARIO=$1
STORAGE=`mktemp -d test-log-XXXXXXXX`

timeout 600 ./abt-io-log-generic-test -f $1 -p $STORAGE
RET=$?

if [ $RET -eq 0 ]
then
    rm -rf $STORAGE
fi
