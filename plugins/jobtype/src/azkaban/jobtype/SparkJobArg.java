package azkaban.jobtype;

public enum SparkJobArg {

  // standard spark submit arguments, ordered in the spark-submit --help order
  MASTER("master", "yarn-cluster", false), // just to trick the eclipse formatter
  DEPLOY_MODE("deploy-mode", false), //
  CLASS("class", false), //
  NAME("name", false), //
  SPARK_JARS("jars", "./lib/*",true), //
  PACKAGES("packages", false), //
  REPOSITORIES("repositories", false), //
  PY_FILES("py-files", false), //
  FILES("files", false), //
  SPARK_CONF_PREFIX("spark.conf.", "--conf", "",true), //
  PROPERTIES_FILE("properties-file", false), //
  DRIVER_MEMORY("driver-memory", "512M", false), //
  DRIVER_JAVA_OPTIONS("driver-java-options", true), //
  DRIVER_LIBRARY_PATH("driver-library-path", false), //
  DRIVER_CLASS_PATH("driver-class-path", false), //
  EXECUTOR_MEMORY("executor-memory", "1g", false), //
  PROXY_USER("proxy-user", false), //
  SPARK_FLAG_PREFIX("spark.flag.", "--", "",true), // --help, --verbose, --supervise, --version

  // Yarn only Arguments
  EXECUTOR_CORES("executor-cores", "1", false), //
  DRIVER_CORES("driver-cores", "1", false), //
  QUEUE("queue", "marathon", false), //
  NUM_EXECUTORS("num-executors", "2", false), //
  ARCHIVES("archives", false), //
  PRINCIPAL("principal", false), //
  KEYTAB("keytab", false), //

  // Not SparkSubmit arguments: only exists in azkaban
  EXECUTION_JAR("execution-jar", null, null,true), //
  PARAMS("params", null, null,true), //
  ;

  public static final String delimiter = "\u001A";

  SparkJobArg(String propName, boolean specialTreatment) {
    this(propName, "--" + propName, "",specialTreatment);
  }

  SparkJobArg(String propName, String defaultValue, boolean specialTreatment) {
    this(propName, "--" + propName, defaultValue,specialTreatment);
  }

  SparkJobArg(String azPropName, String sparkParamName, String defaultValue, boolean specialTreatment) {
    this.azPropName = azPropName;
    this.sparkParamName = sparkParamName;
    this.defaultValue = defaultValue;
    this.needSpecialTreatment = specialTreatment;
  }

  final String azPropName;

  final String sparkParamName;

  final String defaultValue;
  
  final boolean needSpecialTreatment;

}