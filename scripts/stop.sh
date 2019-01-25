#!/usr/bin/env bash

echo "开始stop ....."
num=`ps -ef|grep java|grep flume|wc -l`
if [ "$num" != "0" ] ; then
    #ps -ef|grep java|grep $JAR|awk '{print $2;}'|xargs kill -9
    # 正常停止flume
    ps -ef|grep java|grep flume|awk '{print $2;}'|xargs kill
    echo "进程已经关闭..."
else
    echo "服务未启动，无需停止..."
fi