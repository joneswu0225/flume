#!/bin/bash

THISDIR="$( cd "$( dirname "$0"  )" && pwd  )"
BASEDIR=$(dirname $THISDIR)
JAVA_OPTS="-Xmx1024m -Xms512m -XX:MetaspaceSize=50M -XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:$BASEDIR/logs/gc.log -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$BASEDIR/logs/"
export JAVA_OPTS

echo "this dir,"$THISDIR
echo "basedir,"$BASEDIR

nodeType=$1

if [ -z $nodeType ]; then
   echo "请指定节点类型, 0:前置负载进程, 1:收集进程"
   exit 0
fi

chmod 750 $BASEDIR/bin/flume-ng

if [ $nodeType -eq 0 ]; then
   echo "启动前置http source节点, flume信息监控端口34545."
   nohup $BASEDIR/bin/flume-ng agent --conf $BASEDIR/conf --conf-file $BASEDIR/conf/flume-load_balance_node.properties --name balance -Dflume.root.logger=WARN,console -Dflume.monitoring.type=http -Dflume.monitoring.port=34545 >flume.load_balance.log 2>&1 &
elif [ $nodeType -eq 1 ]; then
   echo "启动收集进程, flume信息监控端口35545." 
   nohup $BASEDIR/bin/flume-ng agent --conf $BASEDIR/conf --conf-file $BASEDIR/conf/flume-collector_node.properties --name producer -Dflume.root.logger=WARN,console -Dflume.monitoring.type=http -Dflume.monitoring.port=35545 >flume.collector.log 2>&1 &
else
   echo "节点类型参数不正确, 0:前置负载进程, 1:收集进程. 传入值为"$nodeType
fi

