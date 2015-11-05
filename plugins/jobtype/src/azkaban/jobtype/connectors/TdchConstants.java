package azkaban.jobtype.connectors;

public interface TdchConstants {
  public static final String MAP_REDUCE_PARAMS =
      "-D mapreduce.map.child.java.opts='-Xmx1G -Djava.security.egd=file:/dev/./urandom -Djava.net.preferIPv4Stack=true' -D mapreduce.job.user.classpath.first=true";
  public static final String TERADATA_JDBCDRIVER_CLASSNAME = "com.teradata.jdbc.TeraDriver";
  public static final String TD_WALLET_FORMAT = "$tdwallet(%s)";
  public static final String TDCH_JOB_TYPE = "hdfs";
  public static final String AVRO_FILE_FORMAT = "avrofile";

  //Keys for the properties
  public static final String TD_WALLET_JAR = "jobtype.tdwallet.jar";
  public static final String LIB_JARS_KEY = "libjars";
  public static final String TD_HOSTNAME_KEY = "td.hostname";
  public static final String TD_USERID_KEY = "td.userid";
  public static final String AVRO_SCHEMA_PATH_KEY = "avro.schema.path";
  public static final String TD_INSERT_METHOD_KEY = "tdch.insert.method";
  public static final String SOURCE_HDFS_PATH_KEY = "source.hdfs.path";
  public static final String TARGET_TD_TABLE_NAME_KEY = "target.td.tablename";
  public static final String HDFS_FILE_FORMAT_KEY = "hdfs.fileformat";
  public static final String HDFS_FIELD_SEPARATOR_KEY = "hdfs.separator";

  public static final String TD_RETRIEVE_METHOD_KEY = "tdch.retrieve.method";
  public static final String SOURCE_TD_TABLE_NAME_KEY = "source.td.tablename";
  public static final String SOURCE_TD_QUERY_NAME_KEY = "source.td.sourcequery";
  public static final String TARGET_HDFS_PATH_KEY = "target.hdfs.path";
}