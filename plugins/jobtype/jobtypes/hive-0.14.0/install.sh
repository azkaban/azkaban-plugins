#!/bin/sh
cd $(dirname $0)
HIVE_HOME=/usr/hdp/2.2.0.0-2041/hive
HADOOP_HOME=/usr/hdp/2.2.0.0-2041/hadoop
MAPREDUCE_HOME=/usr/hdp/2.2.0.0-2041/hadoop-mapreduce
HDFS_HOME=/usr/hdp/2.2.0.0-2041/hadoop-hdfs
cp_lib() {
  cp $HIVE_HOME/lib/$1 lib/
}


mkdir lib

cp_lib "antlr-runtime*"
cp_lib "commons-dbcp*"
cp_lib "commons-pool*"
#cp_lib "datanucleus-connectionpool*"
#api-jdo added instead of connection-pool
cp_lib "datanucleus-api-jdo-*"
cp_lib "datanucleus-core-*"
#cp_lib "datanucleus-enhancer*"
cp_lib "datanucleus-rdbms*"
cp_lib "derby*"
#cp_lib "hive-builtins-0.10.0*"
cp_lib "hive-cli-0.14.0*"
cp_lib "hive-common-0.14.0*"
cp_lib "hive-exec-0.14.0*"
cp_lib "hive-jdbc-0.14.0*"
cp_lib "hive-metastore-0.14.0*"
cp_lib "hive-service-0.14.0*"
cp_lib "hive-shims-0.14.0*"
cp_lib "jdo-api*"
cp_lib "libfb303*"
cp_lib "mysql-*"
cp_lib "guava-*"
cp_lib "*hbase*"
cp_lib "*avro*"
cp_lib "bonecp-0.8.0*"


cp $HADOOP_HOME/client/hadoop-common-2.6.0* lib/
#cp /usr/lib/hadoop/lib/hadoop-lzo-cdh4* lib/
cp $HADOOP_HOME/client/ hadoop-4mc-* lib/
cp $HADOOP_HOME/lib/protobuf-* lib/
cp $HADOOP_HOME/client/hadoop-auth-2.6.0* lib/
cp $MAPREDUCE_HOME/hadoop-mapreduce-client-core-2.6.0* lib/
cp $HDFS_HOME/hadoop-hdfs-2.6.0* lib/

cd -