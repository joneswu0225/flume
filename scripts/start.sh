#!/bin/bash

THISDIR="$( cd "$( dirname "$0"  )" && pwd  )"
BASEDIR=$(dirname $THISDIR)
JAVA_OPTS="-Xmx1024m -Xms512m -XX:MetaspaceSize=50M -XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:$BASEDIR/logs/gc.log -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$BASEDIR/logs/"
export JAVA_OPTS

echo "this dir,"$THISDIR
echo "basedir,"$BASEDIR

echo "JAVAHOME,"$JAVA_HOME
echo "PATH,"$PATH


chmod 750 $THISDIR/bin/flume-ng

echo "启动收集进程, flume信息监控端口35545."
$THISDIR/bin/flume-ng agent --conf $THISDIR/conf --conf-file $THISDIR/conf/flume-load_balance_node.properties --name balance -Dflume.root.logger=INFO,console -Dflume.monitoring.type=http -Dflume.monitoring.port=34545


