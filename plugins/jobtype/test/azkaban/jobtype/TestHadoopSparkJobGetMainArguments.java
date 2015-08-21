package azkaban.jobtype;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import azkaban.jobtype.HadoopSparkJob;
import azkaban.jobtype.SparkJobArg;
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

  String delim = SparkJobArg.delimiter;

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

    // the first one, so no delimiter at front
    Assert.assertTrue(retval.contains("--driver-java-options" + delim));
    Assert.assertTrue(retval
            .contains(delim
                    + "-Dazkaban.link.workflow.url=http://azkaban.link.workflow.url -Dazkaban.link.job.url=http://azkaban.link.job.url -Dazkaban.link.execution.url=http://azkaban.link.execution.url -Dazkaban.link.attempt.url=http://azkaban.link.attempt.url"
                    + delim));
    Assert.assertTrue(retval.contains(delim + "--master" + delim + "yarn-cluster" + delim));
    Assert.assertTrue(retval
            .contains(delim
                    + "--jars"
                    + delim
                    + "/tmp/TestHadoopSpark/./lib/library.jar,/tmp/TestHadoopSpark/./lib/hadoop-spark-job-test-execution-x.y.z-a.b.c.jar"
                    + delim));
    Assert.assertTrue(retval.contains(delim + "--class" + delim
            + "hadoop.spark.job.test.ExecutionClass" + delim));
    Assert.assertTrue(retval.contains(delim + "--num-executors" + delim + "2" + delim));
    Assert.assertTrue(retval.contains(delim + "--executor-cores" + delim + "1" + delim));
    Assert.assertTrue(retval.contains(delim + "--queue" + delim + "marathon" + delim));
    Assert.assertTrue(retval.contains(delim + "--driver-memory" + delim + "2g" + delim));
    Assert.assertTrue(retval.contains(delim + "--executor-memory" + delim + "1g" + delim));
    // last one, no delimiter at back
    Assert.assertTrue(retval.contains(delim
            + "/tmp/TestHadoopSpark/./lib/hadoop-spark-job-test-execution-x.y.z-a.b.c.jar"));

  }

  @Test
  public void testDefaultWithExecutionJarSpecification2() {
    jobProps.put(SparkJobArg.EXECUTION_JAR.azPropName, "./lib/hadoop-spark-job-test-execution");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim
            + "/tmp/TestHadoopSpark/./lib/hadoop-spark-job-test-execution-x.y.z-a.b.c.jar"));
  }

  @Test
  public void testChangeMaster() {
    jobProps.put(SparkJobArg.MASTER.azPropName, "NEW_SPARK_MASTER");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + "--master" + delim + "NEW_SPARK_MASTER" + delim));
    Assert.assertFalse(retval.contains(delim + "--master" + delim + "yarn-cluster" + delim));

  }

  @Test
  public void testChangeSparkJar() throws IOException {
    String topLevelJarString = "topLevelJar.jar";
    File toplevelJarFile = new File(workingDirFile, topLevelJarString);
    toplevelJarFile.createNewFile();
    jobProps.put(SparkJobArg.SPARK_JARS.azPropName, "./*");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + "--jars" + delim + "/tmp/TestHadoopSpark/./"
            + topLevelJarString + delim));
    Assert.assertFalse(retval
            .contains(delim
                    + "--jars"
                    + delim
                    + "/tmp/TestHadoopSpark/./lib/library.jar,/tmp/TestHadoopSpark/./lib/hadoop-spark-job-test-execution-x.y.z-a.b.c.jar"
                    + delim));

  }

  @Test
  public void testExecutionJar() throws IOException {

    jobProps.put(SparkJobArg.EXECUTION_JAR.azPropName, "./lib/library");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + "/tmp/TestHadoopSpark/./lib/library.jar" + delim));

  }

  @Test
  public void testExecutionClass() throws IOException {

    jobProps.put(SparkJobArg.EXECUTION_CLASS.azPropName, "new.ExecutionClass");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + "--class" + delim + "new.ExecutionClass" + delim));
    Assert.assertFalse(retval.contains(delim + "--class" + delim
            + "hadoop.spark.job.test.ExecutionClass" + delim));

  }

  @Test
  public void testNumExecutors() throws IOException {

    jobProps.put(SparkJobArg.NUM_EXECUTORS.azPropName, "1000");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + "--num-executors" + delim + "1000" + delim));
    Assert.assertFalse(retval.contains(delim + "--num-executors" + delim + "2" + delim));
  }

  @Test
  public void testExecutorCores() throws IOException {

    jobProps.put(SparkJobArg.EXECUTOR_CORES.azPropName, "2000");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + "--executor-cores" + delim + "2000" + delim));
    Assert.assertFalse(retval.contains(delim + "--executor-cores" + delim + "1" + delim));

  }

  @Test
  public void testQueue() throws IOException {

    jobProps.put(SparkJobArg.QUEUE.azPropName, "my_own_queue");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + "--queue" + delim + "my_own_queue" + delim));
    Assert.assertFalse(retval.contains(delim + "--queue" + delim + "marathon" + delim));

  }

  @Test
  public void testDriverMemory() throws IOException {

    jobProps.put(SparkJobArg.DRIVER_MEMORY.azPropName, "1t");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + "--driver-memory" + delim + "1t" + delim));
    Assert.assertFalse(retval.contains(delim + "--driver-memory" + delim + "2g" + delim));

  }

  @Test
  public void testExecutorMemory() throws IOException {

    jobProps.put(SparkJobArg.EXECUTOR_MEMORY.azPropName, "1t");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + "--executor-memory" + delim + "1t" + delim));
    Assert.assertFalse(retval.contains(delim + "--executor-memory" + delim + "1g" + delim));
  }

  @Test
  public void testParams() throws IOException {

    jobProps.put(SparkJobArg.PARAMS.azPropName, "param1 param2 param3 param4");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim
            + "/tmp/TestHadoopSpark/./lib/hadoop-spark-job-test-execution-x.y.z-a.b.c.jar" + delim
            + "param1 param2 param3 param4"));

  }

  @Test
  public void testSparkConf() throws IOException {

    jobProps.put(SparkJobArg.SPARK_CONF_PREFIX.azPropName + "conf1", "confValue1");
    jobProps.put(SparkJobArg.SPARK_CONF_PREFIX.azPropName + "conf2", "confValue2");
    jobProps.put(SparkJobArg.SPARK_CONF_PREFIX.azPropName + "conf3", "confValue3");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    for (int i = 1; i <= 3; i++) {
      String confAnswer = String.format(delim + "%s" + delim + "%s%d=%s%d" + delim,
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

    // only on the ending side has the delimiter
    Assert.assertTrue(retval
            .contains("-Dazkaban.link.attempt.url=http://azkaban.link.attempt.url -Dabc=def -Dfgh=ijk"
                    + delim));
  }

  @Test
  public void testSparkDriver() {
    jobProps.put(SparkJobArg.SPARK_DRIVER_PREFIX.azPropName + HadoopSparkJob.DRIVER_JAVA_OPTIONS,
            "-Dabc=def -Dfgh=ijk");
    jobProps.put(SparkJobArg.SPARK_DRIVER_PREFIX.azPropName + "driver-library-path",
            "/new/driver/library/path");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    // only on the ending side has the delimiter
    Assert.assertTrue(retval
            .contains("-Dazkaban.link.attempt.url=http://azkaban.link.attempt.url -Dabc=def -Dfgh=ijk"
                    + delim));
    Assert.assertTrue(retval.contains(delim + "--driver-library-path" + delim
            + "/new/driver/library/path" + delim));
  }

  @Test
  public void testSparkFlag() {
    jobProps.put(SparkJobArg.SPARK_FLAG_PREFIX.azPropName + "help", "doesn't matter 1");
    jobProps.put(SparkJobArg.SPARK_FLAG_PREFIX.azPropName + "verbose", "doesn't matter 2");
    jobProps.put(SparkJobArg.SPARK_FLAG_PREFIX.azPropName + "version", "doesn't matter 3");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains("--help"));
    Assert.assertTrue(retval.contains("--verbose"));
    Assert.assertTrue(retval.contains("--version"));
    Assert.assertFalse(retval.contains("doesn't matter"));
  }
}