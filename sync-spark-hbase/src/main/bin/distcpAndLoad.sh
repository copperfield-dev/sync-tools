#!/bin/sh
source ~/.bashrc

export HADOOP_OPTS="-Xmx50m"
export HADOOP_CLIENT_OPTS="-Xmx50m"
export HBASE_OPTS="-Xmx50m"
export HADOOP_HOME=/home/flume/hadoop/hadoop-client
export HADOOP_CONF_DIR=/home/flume/hadoop/hadoop-client/etc/hadoop/
export HBASE_HOME=/home/flume/hbase/hbase-client
export HBASE_LIBRARY_PATH=/home/flume/hadoop/hadoop-client/lib/native/

hbaseTableName=$1
date=$2

dataReadyFlag=0
echo "Execute: hadoop fs -test -e /tmp/track/sync_db_hfile/ "
${HADOOP_HOME}/bin/hadoop fs -test -e /tmp/track/sync_db_hfile
if [ $? -eq 0 ] ;then
    echo "data file is ready for trans & load, set dataReadyFlag = true & break loop "
    dataReadyFlag=1
else
    echo "success file flag is not generate, sleep loop & check !"
fi

if [ ${dataReadyFlag} -eq 0 ] ;then
	echo "job create hfile failed, system will exit by -1"
	exit -1
fi

# load Hfile
echo "Execute: hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=5000 -Dhbase.bulkload.retries.number=100"
${HBASE_HOME}/bin/hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=5000 -Dhbase.bulkload.retries.number=100 /tmp/track/sync_db_hfile/${date} ${hbaseTableName}
echo "success HFileLoader....."