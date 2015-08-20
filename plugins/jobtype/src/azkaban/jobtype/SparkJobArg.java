package azkaban.jobtype;

public enum SparkJobArg {

  MASTER("master", "--master", "yarn-cluster"), // just to trick the eclipse formatter
  SPARK_JARS("jars", "--jars", "./lib/*"), //
  EXECUTION_JAR("execution.jar", null, null), //
  EXECUTION_CLASS("execution.class", "--class", null), //
  NUM_EXECUTORS("num.executors", "--num-executors", "2"), //
  EXECUTOR_CORES("executor.cores", "--executor-cores", "1"), //
  QUEUE("queue", "--queue", "marathon"), //
  DRIVER_MEMORY("driver.memory", "--driver-memory", "2g"), //
  EXECUTOR_MEMORY("executor.memory", "--executor-memory", "1g"), //
  PARAMS("params", "null", ""), //
  SPARK_CONF_PREFIX("spark.conf.", "--conf", ""), //
  SPARK_DRIVER_PREFIX("spark.driver.", "--", ""), //
  SPARK_FLAGS_PREFIX("spark.flag.", "--", ""); // --help, --verbose, --supervise, --version,
  // --usage-error
  ;

  SparkJobArg(String azPropName, String sparkParamName, String defaultValue) {
    this.azPropName = azPropName;
    this.sparkParamName = sparkParamName;
    this.defaultValue = defaultValue;
  }

  final String azPropName;

  final String sparkParamName;

  final String defaultValue;

}