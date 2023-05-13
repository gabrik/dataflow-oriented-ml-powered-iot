#!/bin/bash

plog () {
   LOG_TS=`eval date "+%F-%T"`
   echo "[$LOG_TS]: $1"
}


HOST=$(hostname)
plog "Going to start flow $FLOW in runtime $RT"

/usr/local/bin/runtime -z /etc/zenoh-flow/zenoh-runtime.json -l /etc/zenoh-flow/py-loader.yaml -g /var/zenoh-flow/flows/$FLOW -r $RT