#!/usr/bin/env bash
source /etc/profile

export TM_SPARK_ROOT=/home/flume/hadoop/spark-client/bin
export SPARK_HOME=/home/flume/hadoop/spark-client
export HADOOP_HOME=/home/flume/hadoop/hadoop-client
export HADOOP_CONF_DIR=/home/flume/hadoop/hadoop-client/etc/hadoop/
export HBASE_HOME=/home/flume/hbase/hbase-client
export HBASE_LIBRARY_PATH=/home/flume/hadoop/hadoop-client/lib/native/

# set shell dir
export base_dir=$(cd `dirname $0`; pwd)

echo "base_dir: ${base_dir}"

# sync-db hfile storage
dateStr=$1
columnFamily=$2
export hfileDir=/tmp/track/sync_db_hfile/${dateStr}
url=$3
filePathPrefix=$4
contentType=application/json
xloginname=$5
xpassword=$6
hbaseTableName=$7

echo "execute generate hfile"
${SPARK_HOME}/bin/spark-submit \
    --class com.nd.da.sync.spark.hbase.HFileGenerateTool \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 1 \
    --executor-memory 5G \
    --executor-cores 1 \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.dynamicAllocation.enabled=false" \
    --driver-memory 1G \
    --verbose \
    ${base_dir}/sync-spark-hbase-1.0-jar-with-dependencies.jar ${dateStr} ${columnFamily} ${hfileDir} ${url} ${filePathPrefix} ${contentType} ${xloginname} ${xpassword}
sh ${base_dir}/distcpAndLoad.sh ${hbaseTableName} ${dateStr}