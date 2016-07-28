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
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import javolution.testing.AssertionException;

import org.apache.hadoop.security.UserGroupInformation;
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
 * 
 * </pre>
 * 
 * @see azkaban.jobtype.HadoopSecureSparkWrapper
 */
public class HadoopSparkJob extends JavaProcessJob {

  // Azkaban/Java params
  private static final String HADOOP_SECURE_SPARK_WRAPPER =
      HadoopSecureSparkWrapper.class.getName();

  // Spark params
  public static final String DRIVER_JAVA_OPTIONS = "driver-java-options";

  // SPARK_HOME ENV VAR
  public static final String SPARK_HOME_ENV_VAR = "SPARK_HOME";
  // SPARK JOBTYPE SYSTEM PROPERTY spark.dynamic.res.alloc.enabled
  public static final String SPARK_DYNAMIC_RES_JOBTYPE_PROPERTY = "spark.dynamic.res.alloc.enabled";
  // SPARK JOBTYPE ENV VAR if spark.dynamic.res.alloc.enabled is set to true
  public static final String SPARK_DYNAMIC_RES_ENV_VAR = "SPARK_DYNAMIC_RES_ENABLED";

  // security variables
  private String userToProxy = null;

  private boolean shouldProxy = false;

  private boolean obtainTokens = false;

  private File tokenFile = null;

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
        hadoopSecurityManager =
            HadoopJobUtils.loadHadoopSecurityManager(getSysProps(), log);
      } catch (RuntimeException e) {
        throw new RuntimeException("Failed to get hadoop security manager!" + e);
      }
    }
  }

  @Override
  public void run() throws Exception {
    HadoopConfigurationInjector.prepareResourcesToInject(getJobProps(),
        getWorkingDirectory());

    if (shouldProxy && obtainTokens) {
      userToProxy = getJobProps().getString(USER_TO_PROXY);
      getLog().info("Need to proxy. Getting tokens.");
      // get tokens in to a file, and put the location in props
      Props props = new Props();
      props.putAll(getJobProps());
      props.putAll(getSysProps());
      tokenFile =
          HadoopJobUtils
              .getHadoopTokens(hadoopSecurityManager, props, getLog());
      getJobProps().put("env." + HADOOP_TOKEN_FILE_LOCATION,
          tokenFile.getAbsolutePath());
    }

    if (getSysProps().getBoolean(SPARK_DYNAMIC_RES_JOBTYPE_PROPERTY, Boolean.FALSE)) {
      getJobProps().put("env." + SPARK_DYNAMIC_RES_ENV_VAR, Boolean.TRUE.toString());
    }

    try {
      super.run();
    } catch (Throwable t) {
      t.printStackTrace();
      getLog().error("caught error running the job");
      throw new Exception(t);
    } finally {
      if (tokenFile != null) {
        HadoopJobUtils.cancelHadoopTokens(hadoopSecurityManager, userToProxy,
            tokenFile, getLog());
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

    String typeUserGlobalJVMArgs =
        getJobProps().getString(HadoopJobUtils.JOBTYPE_GLOBAL_JVM_ARGS, null);
    if (typeUserGlobalJVMArgs != null) {
      args += " " + typeUserGlobalJVMArgs;
    }
    String typeSysGlobalJVMArgs =
        getSysProps().getString(HadoopJobUtils.JOBTYPE_GLOBAL_JVM_ARGS, null);
    if (typeSysGlobalJVMArgs != null) {
      args += " " + typeSysGlobalJVMArgs;
    }
    String typeUserJVMArgs =
        getJobProps().getString(HadoopJobUtils.JOBTYPE_JVM_ARGS, null);
    if (typeUserJVMArgs != null) {
      args += " " + typeUserJVMArgs;
    }
    String typeSysJVMArgs =
        getSysProps().getString(HadoopJobUtils.JOBTYPE_JVM_ARGS, null);
    if (typeSysJVMArgs != null) {
      args += " " + typeSysJVMArgs;
    }

    String typeUserJVMArgs2 =
        getJobProps().getString(HadoopJobUtils.JVM_ARGS, null);
    if (typeUserJVMArgs != null) {
      args += " " + typeUserJVMArgs2;
    }
    String typeSysJVMArgs2 =
        getSysProps().getString(HadoopJobUtils.JVM_ARGS, null);
    if (typeSysJVMArgs != null) {
      args += " " + typeSysJVMArgs2;
    }

    if (shouldProxy) {
      info("Setting up secure proxy info for child process");
      String secure;
      secure =
          " -D" + HadoopSecurityManager.USER_TO_PROXY + "="
              + getJobProps().getString(HadoopSecurityManager.USER_TO_PROXY);
      String extraToken =
          getSysProps().getString(HadoopSecurityManager.OBTAIN_BINARY_TOKEN,
              "false");
      if (extraToken != null) {
        secure +=
            " -D" + HadoopSecurityManager.OBTAIN_BINARY_TOKEN + "="
                + extraToken;
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

  static String testableGetMainArguments(Props jobProps, String workingDir,
      Logger log) {

    // if we ever need to recreate a failure scenario in the test case
    log.debug(jobProps);
    log.debug(workingDir);

    List<String> argList = new ArrayList<String>();

    // special case handling for DRIVER_JAVA_OPTIONS
    argList.add(SparkJobArg.DRIVER_JAVA_OPTIONS.sparkParamName);
    StringBuilder driverJavaOptions = new StringBuilder();
    // note the default java opts are communicated through the hadoop conf and
    // added in the
    // HadoopSecureSparkWrapper
    if (jobProps.containsKey(SparkJobArg.DRIVER_JAVA_OPTIONS.azPropName)) {
      driverJavaOptions.append(" "
          + jobProps.getString(SparkJobArg.DRIVER_JAVA_OPTIONS.azPropName));
    }
    argList.add(driverJavaOptions.toString());

    // Note that execution_jar and params must appear in order, and as the last
    // 2 params
    // Because of the position they are specified in the SparkJobArg class, this
    // should not be an
    // issue
    for (SparkJobArg sparkJobArg : SparkJobArg.values()) {
      if (!sparkJobArg.needSpecialTreatment) {
        handleStandardArgument(jobProps, argList, sparkJobArg);
      } else if (sparkJobArg.equals(SparkJobArg.SPARK_JARS)) {
        sparkJarsHelper(jobProps, workingDir, log, argList);
      } else if (sparkJobArg.equals(SparkJobArg.SPARK_CONF_PREFIX)) {
        sparkConfPrefixHelper(jobProps, argList);
      } else if (sparkJobArg.equals(SparkJobArg.DRIVER_JAVA_OPTIONS)) {
        // do nothing because already handled above
      } else if (sparkJobArg.equals(SparkJobArg.SPARK_FLAG_PREFIX)) {
        sparkFlagPrefixHelper(jobProps, argList);
      } else if (sparkJobArg.equals(SparkJobArg.EXECUTION_JAR)) {
        executionJarHelper(jobProps, workingDir, log, argList);
      } else if (sparkJobArg.equals(SparkJobArg.PARAMS)) {
        paramsHelper(jobProps, argList);
      } else if (sparkJobArg.equals(SparkJobArg.SPARK_VERSION)) {
        // do nothing since this arg is handled in getClassPaths()
      }
    }
    return StringUtils
        .join((Collection<String>) argList, SparkJobArg.delimiter);
  }

  private static void paramsHelper(Props jobProps, List<String> argList) {
    if (jobProps.containsKey(SparkJobArg.PARAMS.azPropName)) {
      String params = jobProps.getString(SparkJobArg.PARAMS.azPropName);
      String[] paramsList = params.split(" ");
      for (String s : paramsList) {
        argList.add(s);
      }
    }
  }

  private static void executionJarHelper(Props jobProps, String workingDir,
      Logger log, List<String> argList) {
    if (jobProps.containsKey(SparkJobArg.EXECUTION_JAR.azPropName)) {
      String executionJarName =
          HadoopJobUtils.resolveExecutionJarName(workingDir,
              jobProps.getString(SparkJobArg.EXECUTION_JAR.azPropName), log);
      argList.add(executionJarName);
    }
  }

  private static void sparkFlagPrefixHelper(Props jobProps, List<String> argList) {
    for (Entry<String, String> entry : jobProps.getMapByPrefix(
        SparkJobArg.SPARK_FLAG_PREFIX.azPropName).entrySet()) {
      if ("true".equalsIgnoreCase(entry.getValue()))
        argList.add(SparkJobArg.SPARK_FLAG_PREFIX.sparkParamName
            + entry.getKey());
    }
  }

  private static void sparkJarsHelper(Props jobProps, String workingDir,
      Logger log, List<String> argList) {
    String propSparkJars =
        jobProps.getString(SparkJobArg.SPARK_JARS.azPropName, "");
    String jarList =
        HadoopJobUtils
            .resolveWildCardForJarSpec(workingDir, propSparkJars, log);
    if (jarList.length() > 0) {
      argList.add(SparkJobArg.SPARK_JARS.sparkParamName);
      argList.add(jarList);
    }
  }

  private static void sparkConfPrefixHelper(Props jobProps, List<String> argList) {
    for (Entry<String, String> entry : jobProps.getMapByPrefix(
        SparkJobArg.SPARK_CONF_PREFIX.azPropName).entrySet()) {
      argList.add(SparkJobArg.SPARK_CONF_PREFIX.sparkParamName);
      String sparkConfKeyVal =
          String.format("%s=%s", entry.getKey(), entry.getValue());
      argList.add(sparkConfKeyVal);
    }
  }

  private static void handleStandardArgument(Props jobProps,
      List<String> argList, SparkJobArg sparkJobArg) {
    if (jobProps.containsKey(sparkJobArg.azPropName)) {
      argList.add(sparkJobArg.sparkParamName);
      argList.add(jobProps.getString(sparkJobArg.azPropName));
    }
  }

  @Override
  protected List<String> getClassPaths() {

    String pluginDir = getSysProps().get("plugin.dir");
    List<String> classPath = super.getClassPaths();

    classPath.add(getSourcePathFromClass(Props.class));
    classPath.add(getSourcePathFromClass(HadoopSecureHiveWrapper.class));
    classPath.add(getSourcePathFromClass(HadoopSecurityManager.class));

    classPath.add(HadoopConfigurationInjector.getPath(getJobProps(),
        getWorkingDirectory()));

    List<String> typeClassPath =
        getSysProps().getStringList("jobtype.classpath", null, ",");
    info("Adding jobtype.classpath: " + typeClassPath);
    if (typeClassPath != null) {
      // fill in this when load this jobtype
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

    String sparkHome = getSparkHome();

    classPath.add(sparkHome + "/conf");
    classPath.add(sparkHome + "/lib/*");

    List<String> typeGlobalClassPath =
        getSysProps().getStringList("jobtype.global.classpath", null, ",");
    info("Adding jobtype.global.classpath: " + typeGlobalClassPath);
    if (typeGlobalClassPath != null) {
      for (String jar : typeGlobalClassPath) {
        if (!classPath.contains(jar)) {
          classPath.add(jar);
        }
      }
    }

    info("Final classpath: " + classPath);
    return classPath;
  }

  private String getSparkHome() {
    String sparkHome = null;
    String jobSparkVer = getJobProps().get(SparkJobArg.SPARK_VERSION.azPropName);
    if (jobSparkVer != null) {
      sparkHome = getSysProps().get("spark." + jobSparkVer + ".home");
      if (sparkHome != null) {
        info("Using job specific spark: " + sparkHome);
        getJobProps().put("env." + SPARK_HOME_ENV_VAR, sparkHome);
      }
    } 

    if (sparkHome == null) {
      sparkHome = getSysProps().get("spark.home");
      info("Using system default spark: " + sparkHome);
    }
    return sparkHome;
  }

  private static String getSourcePathFromClass(Class<?> containedClass) {
    File file =
        new File(containedClass.getProtectionDomain().getCodeSource()
            .getLocation().getPath());

    if (!file.isDirectory() && file.getName().endsWith(".class")) {
      String name = containedClass.getName();
      StringTokenizer tokenizer = new StringTokenizer(name, ".");
      while (tokenizer.hasMoreTokens()) {
        tokenizer.nextElement();
        file = file.getParentFile();
      }

      return file.getPath();
    } else {
      return containedClass.getProtectionDomain().getCodeSource().getLocation()
          .getPath();
    }
  }

  /**
   * This cancel method, in addition to the default canceling behavior, also
   * kills the Spark job on Hadoop
   */
  @Override
  public void cancel() throws InterruptedException {
    super.cancel();

    info("Cancel called.  Killing the Spark job on the cluster");

    String azExecId = jobProps.getString(CommonJobProperties.EXEC_ID);
    final String logFilePath =
        String.format("%s/_job.%s.%s.log", getWorkingDirectory(), azExecId,
            getId());
    info("log file path is: " + logFilePath);

    HadoopJobUtils.proxyUserKillAllSpawnedHadoopJobs(logFilePath, jobProps,
        tokenFile, getLog());
  }
}
