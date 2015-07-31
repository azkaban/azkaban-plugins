cd $(dirname $0)

EXTLIB_DIR=../extlib


cp /usr/lib/hadoop/hadoop-common-2.0.0* $EXTLIB_DIR
cp /usr/lib/hadoop-hdfs/hadoop-hdfs-2.0.0* $EXTLIB_DIR
cp /usr/lib/hadoop/client/hadoop-auth-2.0.0* $EXTLIB_DIR
cp /usr/lib/hadoop/lib/commons-cli* $EXTLIB_DIR
cp /usr/lib/hadoop/lib/commons-collections* $EXTLIB_DIR
cp /usr/lib/hadoop/lib/commons-logging* $EXTLIB_DIR
cp /usr/lib/hadoop/lib/commons-codec* $EXTLIB_DIR
cp /usr/lib/hadoop/lib/commons-lang* $EXTLIB_DIR
cp /usr/lib/hadoop/lib/guava* $EXTLIB_DIR
cp /usr/lib/hadoop/lib/protobuf-java* $EXTLIB_DIR

echo "viewer.external.classpaths="`ls -1 $EXTLIB_DIR |  sed 's#^#extlib/#' | xargs echo | sed 's/ /,/g'` >> plugin.properties
