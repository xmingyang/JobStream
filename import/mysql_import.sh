#!/bin/sh
basepath=$(cd `dirname $0`; pwd)
if [ $# -eq 0 ] 
then
  echo "please set parameter:filename"
  exit 1
fi
filename=$1
host=10.139.230.191
user=jobstreamdb
passwd=3e22d7c74133ab30
db=jobstreamdb
port=4019
#sed  '1d' $filename >$filename.temp
#iconv -f gbk -t utf-8 $filename.temp >$filename.new
python covert_xls.py $filename $filename.tmp
sed  '1d' $filename.tmp | sed 's/"//g' >$filename.new
echo `date` "truncate import"
mysql -h $host -u$user  -p$passwd -P $port -D $db <<EOF
truncate table proj_jobdetail_import;
EOF
if [ $? -ne 0 ] ; then
  echo "清空表proj_jobdetail_import出错"
  exit 1
fi
  
echo `date` "load import"
#character set utf8
mysql -h $host -u$user  -p$passwd -P $port -D $db -e "LOAD DATA LOCAL INFILE '$filename.new' INTO table proj_jobdetail_import character set utf8 FIELDS TERMINATED BY ','  ENCLOSED BY '\"' LINES TERMINATED BY '\n'  (project_id,job_en,job_cn,priority,ip,port,user,path,hdfs_input,hdfs_output,job_type_id,param,owner,after_hour,after_min);"
if [ $? -ne 0 ] ; then
  echo "load表proj_jobdetail_import出错!"
  exit 1
fi
mysql -h $host -u$user  -p$passwd -P $port -D $db <<EOF
truncate table proj_jobdetail_bak;
insert into proj_jobdetail_bak select * from proj_jobdetail;
EOF
if [ $? -ne 0 ] ; then
  echo "备份出错!"
  exit 1
fi

mysql -h $host -u$user  -p$passwd -P $port -D $db <<EOF
truncate table proj_jobdetail;
insert into proj_jobdetail select * from proj_jobdetail_import;
EOF
if [ $? -ne 0 ] ; then
  echo "正式导入出错!"
  exit 1
fi
echo "完成导入!"
