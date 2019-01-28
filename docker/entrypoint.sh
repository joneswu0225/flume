#!/usr/bin/env bash
set -e
# 详情见 http://confluence.datayes.com/pages/viewpage.action?pageId=24242117
source /usr/bin/datayes-init

ls -l

curl ${params}/applog-flume/flume-load_balance_node.properties?raw > /datayes/applog-flume/conf/flume-load_balance_node.properties

# 用于程序启动逻辑
set +e

ls -l conf/

while read line
do
    echo ${line}
done < conf/flume-load_balance_node.properties
echo "finish export envs"

./start.sh