#!/bin/sh
#usage 1: java -cp jobStream-0.0.1-SNAPSHOT.jar org.jobstream.JobStream project_en=xx
#usage 2: java -cp jobStream-0.0.1-SNAPSHOT.jar org.jobstream.JobStream project_en=xx joblist=job1,job2 is_dag=false|true
#usage 3: java -cp jobStream-0.0.1-SNAPSHOT.jar org.jobstream.JobStream project_en=xx is_last_rerun=true last_scheduler_seq=zz

cd $JOBSTREAM_HOME
java -cp jobStream-0.0.1-SNAPSHOT.jar org.jobstream.JobStream $*
#java -cp jobStream-0.0.1-SNAPSHOT.jar org.jobstream.JobStream project_en=proj_test
#java -cp jobStream-0.0.1-SNAPSHOT.jar org.jobstream.JobStream project_en=proj_test joblist=pythontest is_dag=false
