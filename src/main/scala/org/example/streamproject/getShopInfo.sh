#!/bin/bash
streaming_dir="/home/briup/streamproject/data/"
#HDFS="/opt/hadoop/bin/hadoop fs"
#delete hdfs files
#$HDFS -rm "${streaming_dir}"'/tmp/*' > /dev/null 2>&1
#$HDFS -rm "${streaming_dir}"'/*' > /dev/null 2>&1
while [ 1 ]; do
 scala SimulatorFile.sc > test.log
 tmplog="access.`date +'%s'`.log"
 #Generate data to HDFS
 #$HDFS -put test.log ${streaming_dir}/tmp/$tmplog
 #$HDFS -mv ${streaming_dir}/tmp/$tmplog ${streaming_dir}/
 #echo "`date +"%F %T"` put $tmplog to HDFS succeed"
 #Generate data to LOCAL
 cp test.log ${streaming_dir}/tmp/$tmplog
 mv ${streaming_dir}/tmp/$tmplog ${streaming_dir}/
 echo "`date +"%F %T"` generating $tmplog LOCAL succeed"
 sleep 1
done