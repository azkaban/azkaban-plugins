package azkaban.jobtype;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import azkaban.utils.Props;

public class TestHadoopSparkJobGetMainArguments {
  Props jobProps = null;

  Logger logger = Logger.getRootLogger();

  String workingDirString = "/tmp/TestHadoopSpark";

  File workingDirFile = new File(workingDirString);

  File libFolderFile = new File(workingDirFile, "lib");

  String executionJarName = "hadoop-spark-job-test-execution-x.y.z-a.b.c.jar";

  File executionJarFile = new File(libFolderFile, "hadoop-spark-job-test-execution-x.y.z-a.b.c.jar");

  File libraryJarFile = new File(libFolderFile, "library.jar");

  @BeforeTest
  public void beforeTest() throws IOException {

  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    if (workingDirFile.exists())
      FileUtils.deleteDirectory(workingDirFile);
    workingDirFile.mkdirs();
    libFolderFile.mkdirs();
    executionJarFile.createNewFile();
    libraryJarFile.createNewFile();

    jobProps = new Props();
    jobProps.put("azkaban.link.workflow.url", "http://azkaban.link.workflow.url");
    jobProps.put("azkaban.link.job.url", "http://azkaban.link.job.url");
    jobProps.put("azkaban.link.execution.url", "http://azkaban.link.execution.url");
    jobProps.put("azkaban.link.jobexec.url", "http://azkaban.link.jobexec.url");
    jobProps.put("azkaban.link.attempt.url", "http://azkaban.link.attempt.url");
    jobProps.put(SparkJobArg.EXECUTION_CLASS.azPropName, "hadoop.spark.job.test.ExecutionClass");
    jobProps.put(SparkJobArg.EXECUTION_JAR.azPropName, "./lib/hadoop-spark-job-test-execution.jar");

  }

  @Test
  public void testDefault() {
    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval
            .contains("--driver-java-options -Dazkaban.link.workflow.url=http://azkaban.link.workflow.url"));
    Assert.assertTrue(retval.contains("-Dazkaban.link.job.url=http://azkaban.link.job.url"));
    Assert.assertTrue(retval
            .contains("-Dazkaban.link.execution.url=http://azkaban.link.execution.url"));
    Assert.assertTrue(retval.contains("-Dazkaban.link.attempt.url=http://azkaban.link.attempt.url"));
    Assert.assertTrue(retval.contains("--master yarn-cluster"));
    Assert.assertTrue(retval
            .contains("--jars /tmp/TestHadoopSpark/./lib/library.jar,/tmp/TestHadoopSpark/./lib/hadoop-spark-job-test-execution-x.y.z-a.b.c.jar"));
    Assert.assertTrue(retval.contains("--class hadoop.spark.job.test.ExecutionClass"));
    Assert.assertTrue(retval.contains("--num-executors 2"));
    Assert.assertTrue(retval.contains("--executor-cores 1"));
    Assert.assertTrue(retval.contains("--queue marathon"));
    Assert.assertTrue(retval.contains("--driver-memory 2g"));
    Assert.assertTrue(retval.contains("--executor-memory 1g"));
    Assert.assertTrue(retval
            .contains("/tmp/TestHadoopSpark/./lib/hadoop-spark-job-test-execution-x.y.z-a.b.c.jar"));

  }

  @Test
  public void testDefaultWithExecutionJarSpecification2() {
    jobProps.put(SparkJobArg.EXECUTION_JAR.azPropName, "./lib/hadoop-spark-job-test-execution");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval
            .contains("/tmp/TestHadoopSpark/./lib/hadoop-spark-job-test-execution-x.y.z-a.b.c.jar"));
  }

  @Test
  public void testChangeMaster() {
    jobProps.put(SparkJobArg.MASTER.azPropName, "NEW_SPARK_MASTER");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains("--master NEW_SPARK_MASTER"));
    Assert.assertFalse(retval.contains("--master yarn-cluster"));

  }

  @Test
  public void testChangeSparkJar() throws IOException {
    String topLevelJarString = "topLevelJar.jar";
    File toplevelJarFile = new File(workingDirFile, topLevelJarString);
    toplevelJarFile.createNewFile();
    jobProps.put(SparkJobArg.SPARK_JARS.azPropName, "./*");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains("--jars /tmp/TestHadoopSpark/./" + topLevelJarString));
    Assert.assertFalse(retval
            .contains("--jars /tmp/TestHadoopSpark/./lib/library.jar,/tmp/TestHadoopSpark/./lib/hadoop-spark-job-test-execution-x.y.z-a.b.c.jar"));

  }

  @Test
  public void testExecutionJar() throws IOException {

    jobProps.put(SparkJobArg.EXECUTION_JAR.azPropName, "./lib/library");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains("/tmp/TestHadoopSpark/./lib/library.jar"));    

  }

  @Test
  public void testExecutionClass() throws IOException {

    jobProps.put(SparkJobArg.EXECUTION_CLASS.azPropName, "new.ExecutionClass");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains("--class new.ExecutionClass"));
    Assert.assertFalse(retval.contains("--class hadoop.spark.job.test.ExecutionClass"));

  }

  @Test
  public void testNumExecutors() throws IOException {

    jobProps.put(SparkJobArg.NUM_EXECUTORS.azPropName, "1000");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains("--num-executors 1000"));
    Assert.assertFalse(retval.contains("--num-executors 2"));
  }

  @Test
  public void testExecutorCores() throws IOException {

    jobProps.put(SparkJobArg.EXECUTOR_CORES.azPropName, "2000");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains("--executor-cores 2000"));
    Assert.assertFalse(retval.contains("--executor-cores 1"));

  }

  @Test
  public void testQueue() throws IOException {

    jobProps.put(SparkJobArg.QUEUE.azPropName, "my_own_queue");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains("--queue my_own_queue"));
    Assert.assertFalse(retval.contains("--queue marathon"));

  }

  @Test
  public void testDriverMemory() throws IOException {

    jobProps.put(SparkJobArg.DRIVER_MEMORY.azPropName, "1t");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains("--driver-memory 1t"));
    Assert.assertFalse(retval.contains("--driver-memory 2g"));

  }

  @Test
  public void testExecutorMemory() throws IOException {

    jobProps.put(SparkJobArg.EXECUTOR_MEMORY.azPropName, "1t");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains("--executor-memory 1t"));
    Assert.assertFalse(retval.contains("--executor-memory 1g"));
  }

  @Test
  public void testParams() throws IOException {

    jobProps.put(SparkJobArg.PARAMS.azPropName, "param1 param2 param3 param4");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval
            .contains("/tmp/TestHadoopSpark/./lib/hadoop-spark-job-test-execution-x.y.z-a.b.c.jar param1 param2 param3 param4"));

  }

  @Test
  public void testSparkConf() throws IOException {

    jobProps.put(SparkJobArg.SPARK_CONF_PREFIX.azPropName + "conf1", "confValue1");
    jobProps.put(SparkJobArg.SPARK_CONF_PREFIX.azPropName + "conf2", "confValue2");
    jobProps.put(SparkJobArg.SPARK_CONF_PREFIX.azPropName + "conf3", "confValue3");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    for (int i = 1; i <= 3; i++) {
      String confAnswer = String.format("%s %s%d=%s%d",
              SparkJobArg.SPARK_CONF_PREFIX.sparkParamName, "conf", i, "confValue", i);
      System.out.println("looking for: " + confAnswer);
      Assert.assertTrue(retval.contains(confAnswer));
    }

  }

  @Test
  public void testAdditionalDriverJavaOptions() {
    jobProps.put(SparkJobArg.SPARK_DRIVER_PREFIX.azPropName + HadoopSparkJob.DRIVER_JAVA_OPTIONS,
            "-Dabc=def -Dfgh=ijk");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval
            .contains("-Dazkaban.link.attempt.url=http://azkaban.link.attempt.url -Dabc=def -Dfgh=ijk"));
  }

  @Test
  public void testSparkDriver() {
    jobProps.put(SparkJobArg.SPARK_DRIVER_PREFIX.azPropName + HadoopSparkJob.DRIVER_JAVA_OPTIONS,
            "-Dabc=def -Dfgh=ijk");
    jobProps.put(SparkJobArg.SPARK_DRIVER_PREFIX.azPropName + "driver-library-path",
            "/new/driver/library/path");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval
            .contains("-Dazkaban.link.attempt.url=http://azkaban.link.attempt.url -Dabc=def -Dfgh=ijk"));
    Assert.assertTrue(retval.contains("--driver-library-path /new/driver/library/path"));
  }

  @Test
  public void testSparkFlag() {
    jobProps.put(SparkJobArg.SPARK_FLAGS_PREFIX.azPropName + "help", "doesn't matter 1");
    jobProps.put(SparkJobArg.SPARK_FLAGS_PREFIX.azPropName + "verbose", "doesn't matter 2");
    jobProps.put(SparkJobArg.SPARK_FLAGS_PREFIX.azPropName + "version", "doesn't matter 3");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains("--help"));
    Assert.assertTrue(retval.contains("--verbose"));
    Assert.assertTrue(retval.contains("--version"));
    Assert.assertFalse(retval.contains("doesn't matter"));
  }
}