#!/bin/sh
cd $JOBSTREAM_HOME
pid=`ps -ef|grep org.jobstream.MainScheduler|grep -v grep|grep -v PPID|awk '{ print $2}'`
if [[ $pid -gt 0 ]]
then 
echo "MainScheduler pid" $pid" exist ,please stop it first"
exit
fi
echo "MainScheduler Starting..."
nohup java -Xms256m -Xmx2048m -cp jobStream-0.0.1-SNAPSHOT.jar org.jobstream.MainScheduler >$JOBSTREAM_HOME/logs/start.log 2>&1 &
echo "MainScheduler Started"
