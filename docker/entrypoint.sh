#!/usr/bin/env bash
set -e
source /usr/bin/datayes-init
ls -l
echo ${params}

curl ${params}/flume/flume-load_balance_node.properties?raw > flume-load_balance_node.properties

ls -l conf/
while read line
do
    echo ${line}
done < config/application.properties
echo "finish export envs"

./start.sh