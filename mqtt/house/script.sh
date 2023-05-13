#!/bin/bash

DOCKERINFO=$(curl -s --unix-socket /run/docker.sock http://docker/containers/$HOSTNAME/json)

export ID=$(python3 -c "import sys, json; print(json.loads(sys.argv[1])[\"Name\"].split(\"_\")[-1])" "$DOCKERINFO")

echo $(cut -c9- < /proc/1/cpuset)

python runnable.py