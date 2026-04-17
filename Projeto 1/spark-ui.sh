#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <event_log_directory>"
    exit 1
fi

if [ ! -d "$1" ]; then
    echo "Error: $1 is not a directory"
    exit 1
fi

pyspark --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="$1"
