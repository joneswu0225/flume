#!/usr/bin/env bash
set -e
source /usr/bin/datayes-init
ls -l

curl ${params}/flume/flume-load_balance_node.properties?raw > conf/flume-load_balance_node.properties

# 用于程序启动逻辑
set +e

ls -l conf/
while read line
do
    echo ${line}
done < flume-load_balance_node.properties
echo "finish export envs"

./start.sh