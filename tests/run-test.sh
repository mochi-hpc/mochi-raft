#!/bin/bash

SCENARIO=$1
BACKEND=$2

SCENARIO_FILE=$(basename "$SCENARIO")
SCENARIO_NAME="${SCENARIO_FILE%.*}-${BACKEND}"

STORAGE=`mktemp -d ${SCENARIO_NAME}-XXXXXXXX`

timeout 60 ./mraft-test na+sm \
    -n 3                      \
    -f $SCENARIO              \
    -p $STORAGE               \
    -l $BACKEND               \
    -w trace                  \
    -t $STORAGE
RET=$?

if [ ! -e "results.tar" ]; then
    tar --create --file=results.tar
fi

tar --append --file=results.tar $STORAGE

rm -rf $STORAGE

exit $RET
