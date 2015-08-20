/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.jobtype;

import static azkaban.flow.CommonJobProperties.ATTEMPT_LINK;
import static azkaban.flow.CommonJobProperties.EXECUTION_LINK;
import static azkaban.flow.CommonJobProperties.JOB_LINK;
import static azkaban.flow.CommonJobProperties.WORKFLOW_LINK;
import static azkaban.security.commons.HadoopSecurityManager.ENABLE_PROXYING;
import static azkaban.security.commons.HadoopSecurityManager.OBTAIN_BINARY_TOKEN;
import static azkaban.security.commons.HadoopSecurityManager.USER_TO_PROXY;
import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import javolution.testing.AssertionException;

import org.apache.log4j.Logger;

import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.JavaProcessJob;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;
import azkaban.utils.StringUtils;

/**
 * <pre>
 * The Azkaban adaptor for running a Spark Submit job.
 * Use this in conjunction with  {@link azkaban.jobtype.HadoopSecureSparkWrapper}
 *
 * </pre>
 * 
 * @see azkaban.jobtype.HadoopSecureSparkWrapper
 */
public class HadoopSparkJob extends JavaProcessJob {

  // Azkaban/Java params
  private static final String HADOOP_SECURE_SPARK_WRAPPER = HadoopSecureSparkWrapper.class
          .getName();

  // Spark params

  public static final String DRIVER_JAVA_OPTIONS = "driver-java-options";

  // security variables
  private String userToProxy = null;

  private boolean shouldProxy = false;

  private boolean obtainTokens = false;

  private HadoopSecurityManager hadoopSecurityManager;

  public HadoopSparkJob(String jobid, Props sysProps, Props jobProps, Logger log) {
    super(jobid, sysProps, jobProps, log);

    getJobProps().put(CommonJobProperties.JOB_ID, jobid);

    shouldProxy = getSysProps().getBoolean(ENABLE_PROXYING, false);
    getJobProps().put(ENABLE_PROXYING, Boolean.toString(shouldProxy));
    obtainTokens = getSysProps().getBoolean(OBTAIN_BINARY_TOKEN, false);

    if (shouldProxy) {
      getLog().info("Initiating hadoop security manager.");
      try {
        hadoopSecurityManager = HadoopJobUtils.loadHadoopSecurityManager(getSysProps(), log);
      } catch (RuntimeException e) {
        throw new RuntimeException("Failed to get hadoop security manager!" + e);
      }
    }
  }

  @Override
  public void run() throws Exception {
    HadoopConfigurationInjector.prepareResourcesToInject(getJobProps(), getWorkingDirectory());

    File tokenFile = null;
    if (shouldProxy && obtainTokens) {
      userToProxy = getJobProps().getString(USER_TO_PROXY);
      getLog().info("Need to proxy. Getting tokens.");
      // get tokens in to a file, and put the location in props
      Props props = new Props();
      props.putAll(getJobProps());
      props.putAll(getSysProps());
      tokenFile = HadoopJobUtils.getHadoopTokens(hadoopSecurityManager, props, getLog());
      getJobProps().put("env." + HADOOP_TOKEN_FILE_LOCATION, tokenFile.getAbsolutePath());
    }

    try {
      super.run();
    } catch (Throwable t) {
      t.printStackTrace();
      getLog().error("caught error running the job");
      throw new Exception(t);
    } finally {
      if (tokenFile != null) {
        HadoopJobUtils.cancelHadoopTokens(hadoopSecurityManager, userToProxy, tokenFile, getLog());
        if (tokenFile.exists()) {
          tokenFile.delete();
        }
      }
    }
  }

  @Override
  protected String getJavaClass() {
    return HADOOP_SECURE_SPARK_WRAPPER;
  }

  @Override
  protected String getJVMArguments() {
    String args = super.getJVMArguments();

    String typeUserGlobalJVMArgs = getJobProps().getString("jobtype.global.jvm.args", null);
    if (typeUserGlobalJVMArgs != null) {
      args += " " + typeUserGlobalJVMArgs;
    }
    String typeSysGlobalJVMArgs = getSysProps().getString("jobtype.global.jvm.args", null);
    if (typeSysGlobalJVMArgs != null) {
      args += " " + typeSysGlobalJVMArgs;
    }
    String typeUserJVMArgs = getJobProps().getString("jobtype.jvm.args", null);
    if (typeUserJVMArgs != null) {
      args += " " + typeUserJVMArgs;
    }
    String typeSysJVMArgs = getSysProps().getString("jobtype.jvm.args", null);
    if (typeSysJVMArgs != null) {
      args += " " + typeSysJVMArgs;
    }

    String typeUserJVMArgs2 = getJobProps().getString("jvm.args", null);
    if (typeUserJVMArgs != null) {
      args += " " + typeUserJVMArgs2;
    }
    String typeSysJVMArgs2 = getSysProps().getString("jvm.args", null);
    if (typeSysJVMArgs != null) {
      args += " " + typeSysJVMArgs2;
    }

    if (shouldProxy) {
      info("Setting up secure proxy info for child process");
      String secure;
      secure = " -D" + HadoopSecurityManager.USER_TO_PROXY + "="
              + getJobProps().getString(HadoopSecurityManager.USER_TO_PROXY);
      String extraToken = getSysProps().getString(HadoopSecurityManager.OBTAIN_BINARY_TOKEN,
              "false");
      if (extraToken != null) {
        secure += " -D" + HadoopSecurityManager.OBTAIN_BINARY_TOKEN + "=" + extraToken;
      }
      info("Secure settings = " + secure);
      args += secure;
    } else {
      info("Not setting up secure proxy info for child process");
    }

    return args;
  }

  @Override
  protected String getMainArguments() {
    return testableGetMainArguments(jobProps, getWorkingDirectory(), getLog());
  }

  static String testableGetMainArguments(Props jobProps, String workingDir, Logger log) {

    // if we ever need to recreate a failure scenario in the test case
    log.debug(jobProps);
    log.debug(workingDir);

    ArrayList<String> argList = new ArrayList<String>();

    // special case handling for DRIVER_JAVA_OPTIONS
    argList.add("--" + DRIVER_JAVA_OPTIONS);
    argList.add(HadoopJobUtils.javaOptStringFromAzkabanProps(jobProps, WORKFLOW_LINK));
    argList.add(HadoopJobUtils.javaOptStringFromAzkabanProps(jobProps, JOB_LINK));
    argList.add(HadoopJobUtils.javaOptStringFromAzkabanProps(jobProps, EXECUTION_LINK));
    argList.add(HadoopJobUtils.javaOptStringFromAzkabanProps(jobProps, ATTEMPT_LINK));
    String azDriverJavaOptions = SparkJobArg.SPARK_DRIVER_PREFIX.azPropName + DRIVER_JAVA_OPTIONS;
    if (jobProps.containsKey(azDriverJavaOptions)) {
      argList.add(jobProps.getString(azDriverJavaOptions));
    }

    argList.add(SparkJobArg.MASTER.sparkParamName);
    argList.add(jobProps.getString(SparkJobArg.MASTER.azPropName, SparkJobArg.MASTER.defaultValue));

    String jarList = HadoopJobUtils.resolveWildCardForJarSpec(workingDir, jobProps.getString(
            SparkJobArg.SPARK_JARS.azPropName, SparkJobArg.SPARK_JARS.defaultValue), log);
    if (jarList != null && jarList.length() > 0) {
      argList.add(SparkJobArg.SPARK_JARS.sparkParamName);
      argList.add(jarList);
    }

    argList.add(SparkJobArg.EXECUTION_CLASS.sparkParamName);
    argList.add(jobProps.getString(SparkJobArg.EXECUTION_CLASS.azPropName));

    argList.add(SparkJobArg.NUM_EXECUTORS.sparkParamName);
    argList.add(jobProps.getString(SparkJobArg.NUM_EXECUTORS.azPropName,
            SparkJobArg.NUM_EXECUTORS.defaultValue));

    argList.add(SparkJobArg.EXECUTOR_CORES.sparkParamName);
    argList.add(jobProps.getString(SparkJobArg.EXECUTOR_CORES.azPropName,
            SparkJobArg.EXECUTOR_CORES.defaultValue));

    argList.add(SparkJobArg.QUEUE.sparkParamName);
    argList.add(jobProps.getString(SparkJobArg.QUEUE.azPropName, SparkJobArg.QUEUE.defaultValue));

    argList.add(SparkJobArg.DRIVER_MEMORY.sparkParamName);
    argList.add(jobProps.getString(SparkJobArg.DRIVER_MEMORY.azPropName,
            SparkJobArg.DRIVER_MEMORY.defaultValue));

    argList.add(SparkJobArg.EXECUTOR_MEMORY.sparkParamName);
    argList.add(jobProps.getString(SparkJobArg.EXECUTOR_MEMORY.azPropName,
            SparkJobArg.EXECUTOR_MEMORY.defaultValue));

    for (Entry<String, String> entry : jobProps.getMapByPrefix(
            SparkJobArg.SPARK_DRIVER_PREFIX.azPropName).entrySet()) {
      argList.add(SparkJobArg.SPARK_DRIVER_PREFIX.sparkParamName + entry.getKey());
      argList.add(entry.getValue());
    }

    for (Entry<String, String> entry : jobProps.getMapByPrefix(
            SparkJobArg.SPARK_FLAGS_PREFIX.azPropName).entrySet()) {
      argList.add(SparkJobArg.SPARK_FLAGS_PREFIX.sparkParamName + entry.getKey());
    }

    for (Entry<String, String> entry : jobProps.getMapByPrefix(
            SparkJobArg.SPARK_CONF_PREFIX.azPropName).entrySet()) {
      argList.add(SparkJobArg.SPARK_CONF_PREFIX.sparkParamName);
      String sparkConfKeyVal = String.format("%s=%s", entry.getKey(), entry.getValue());
      argList.add(sparkConfKeyVal);
    }

    String executionJarName = HadoopJobUtils.resolveExecutionJarName(workingDir,
            jobProps.getString(SparkJobArg.EXECUTION_JAR.azPropName), log);
    argList.add(executionJarName);

    argList.add(jobProps.getString(SparkJobArg.PARAMS.azPropName, SparkJobArg.PARAMS.defaultValue));

    return StringUtils.join((Collection<String>) argList, " ");
  }

  @Override
  protected List<String> getClassPaths() {

    List<String> classPath = super.getClassPaths();

    classPath.add(getSourcePathFromClass(Props.class));
    classPath.add(getSourcePathFromClass(HadoopSecureHiveWrapper.class));
    classPath.add(getSourcePathFromClass(HadoopSecurityManager.class));

    classPath.add(HadoopConfigurationInjector.getPath(getJobProps(), getWorkingDirectory()));
    List<String> typeClassPath = getSysProps().getStringList("jobtype.classpath", null, ",");
    if (typeClassPath != null) {
      // fill in this when load this jobtype
      String pluginDir = getSysProps().get("plugin.dir");
      for (String jar : typeClassPath) {
        File jarFile = new File(jar);
        if (!jarFile.isAbsolute()) {
          jarFile = new File(pluginDir + File.separatorChar + jar);
        }

        if (!classPath.contains(jarFile.getAbsoluteFile())) {
          classPath.add(jarFile.getAbsolutePath());
        }
      }
    }

    List<String> typeGlobalClassPath = getSysProps().getStringList("jobtype.global.classpath",
            null, ",");
    if (typeGlobalClassPath != null) {
      for (String jar : typeGlobalClassPath) {
        if (!classPath.contains(jar)) {
          classPath.add(jar);
        }
      }
    }

    return classPath;
  }

  private static String getSourcePathFromClass(Class<?> containedClass) {
    File file = new File(containedClass.getProtectionDomain().getCodeSource().getLocation()
            .getPath());

    if (!file.isDirectory() && file.getName().endsWith(".class")) {
      String name = containedClass.getName();
      StringTokenizer tokenizer = new StringTokenizer(name, ".");
      while (tokenizer.hasMoreTokens()) {
        tokenizer.nextElement();
        file = file.getParentFile();
      }

      return file.getPath();
    } else {
      return containedClass.getProtectionDomain().getCodeSource().getLocation().getPath();
    }
  }

  /**
   * This cancel method, in addition to the default canceling behavior, also kills the Spark job on
   * Hadoop
   */
  @Override
  public void cancel() throws InterruptedException {
    super.cancel();

    String azExecId = jobProps.getString("azkaban.flow.execid");
    String logFileName = String.format("%s/_job.%s.%s.log", getWorkingDirectory(), azExecId,
            getId());
    debug("log file name is: " + logFileName);

    String applicationId = findApplicationIdFromLog(logFileName);
    debug("applicationId is: " + applicationId);
    if (applicationId == null)
      return;

    HadoopJobUtils.killJobOnCluster(applicationId, getLog());
  }

  /**
   * <pre>
   * This is in effect does a grep of the log file, and parse out the application_xxx_yyy string
   * </pre>
   * 
   * @param logFileName
   * @return
   */
  private String findApplicationIdFromLog(String logFileName) {
    String applicationId = null;
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(logFileName));
      String input;
      while ((input = br.readLine()) != null) {
        if (input.contains("Submitted application"))
          break;
      }
      if (input == null) {
        return null;
      }
      info("found input: " + input);
      String[] inputSplit = input.split(" ");
      if (inputSplit.length < 2) {
        info("cannot find application id");
        return null;
      }
      applicationId = inputSplit[inputSplit.length - 1];

      if (!applicationId.startsWith("application")) {
        throw new AssertionException(
                "Internal implementation err: applicationId does not start with application: "
                        + applicationId);
      }
      info("application ID is: " + applicationId);
      return applicationId;
    } catch (IOException e) {
      e.printStackTrace();
      info("Error while trying to find applicationId for Spark log", e);
    } finally {
      try {
        if (br != null)
          br.close();
      } catch (Exception e) {
        // do nothing
      }
    }
    return applicationId;
  }
}
