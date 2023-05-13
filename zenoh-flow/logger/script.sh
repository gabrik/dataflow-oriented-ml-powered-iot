#!/bin/bash
TS=$(date +%Y%m%d.%H%M%S)
LOG_FILE="/results/$FILE-$TS.txt"
CMD="python runnable.py -m client -e tcp/zenoh-router:7447 -k $KEY -f $LOG_FILE"
echo "$HOSTNAME is going to run: $CMD"
python runnable.py -m client -e tcp/zenoh-router:7447 -k $KEY -f $LOG_FILE