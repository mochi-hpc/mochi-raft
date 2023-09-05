#!/bin/bash

SCENARIO=$1
BACKEND=$2

SCENARIO_FILE=$(basename "$SCENARIO")
SCENARIO_NAME="${SCENARIO_FILE%.*}-${BACKEND}"

STORAGE=`mktemp -d storage-${SCENARIO_NAME}-XXXXXXXX`

timeout 60 ./mraft-py-test na+sm \
    -n 3                         \
    -f $SCENARIO                 \
    -p $STORAGE                  \
    -l $BACKEND                  \
    -w trace                     \
    -t $STORAGE
RET=$?

if [ "$RET" -eq "0" ]; then
    rm -rf $STORAGE
fi

exit $RET
