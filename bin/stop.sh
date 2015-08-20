#!/bin/sh
cd $JOBSTREAM_HOME
pid=`ps -ef|grep org.jobstream.MainScheduler|grep -v grep|grep -v PPID|awk '{ print $2}'`
if [[ $pid -gt 0 ]]
then 
echo "MainScheduler Stopping..."
kill -9 $pid
echo "MainScheduler Stopped"
else
echo "MainScheduler Not Exist"
fi
