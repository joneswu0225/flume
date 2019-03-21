#!/usr/bin/env bash

echo "开始stop ....."
BASEDIR=`pwd`
num=`ps -ef|grep ${BASEDIR}|wc -l`
if [ "$num" != "0" ] ; then
    #ps -ef|grep java|grep $JAR|awk '{print $2;}'|xargs kill -9
    # 正常停止flume
    ps -ef|grep `pwd`|grep java|awk '{print $2}'|xargs kill -9
    echo "进程已经关闭..."
else
    echo "服务未启动，无需停止..."
fi