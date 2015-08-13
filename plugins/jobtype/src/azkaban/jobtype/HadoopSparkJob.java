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

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import javolution.testing.AssertionException;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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

  private static final Logger logger = Logger.getRootLogger();

  // Spark params
  private static final String MASTER = "master";

  private static final String SPARK_JARS = "jars";

  private static final String EXECUTION_JAR_AZKABAN = "execution.jar";

  private static final String EXECUTION_CLASS_AZKABAN = "execution.class";

  private static final String EXECUTION_CLASS_SPARK = "class";

  private static final String NUM_EXECUTORS_AZKABAN = "num.executors";

  private static final String NUM_EXECUTORS_SPARK = "num-executors";

  private static final String EXECUTOR_CORES_AZKABAN = "executor.cores";

  private static final String EXECUTOR_CORES_SPARK = "executor-cores";

  private static final String QUEUE = "queue";

  private static final String DRIVER_MEMORY_AZKABAN = "driver.memory";

  private static final String DRIVER_MEMORY_SPARK = "driver-memory";

  private static final String EXECUTOR_MEMORY_AZKABAN = "executor.memory";

  private static final String EXECUTOR_MEMORY_SPARK = "executor-memory";

  private static final String PARAMS = "params";

  private static final String SPARK_CONF_AZKABAN = "spark.conf.";

  private static final String SPARK_CONF_SPARK = "conf";

  // Spark default
  private static final String MASTER_DEFAULT = "yarn-cluster";

  private static final String SPARK_JARS_DEFAULT = "./lib/*";

  private static final int NUM_EXECUTORS_DEFAULT = 2;

  private static final int EXECUTOR_CORES_DEFAULT = 1;

  private static final String QUEUE_DEFAULT = "marathon";

  private static final String DRIVER_MEMORY_DEFAULT = "2g";

  private static final String EXECUTOR_MEMORY_DEFAULT = "1g";

  // security variables
  private String userToProxy = null;

  private boolean shouldProxy = false;

  private boolean obtainTokens = false;

  private HadoopSecurityManager hadoopSecurityManager;


  public HadoopSparkJob(String jobid, Props sysProps, Props jobProps, Logger log) {
    super(jobid, sysProps, jobProps, log);

    getJobProps().put(CommonJobProperties.JOB_ID, jobid);

    shouldProxy = getSysProps().getBoolean(HadoopSecurityManager.ENABLE_PROXYING, false);
    getJobProps().put(HadoopSecurityManager.ENABLE_PROXYING, Boolean.toString(shouldProxy));
    obtainTokens = getSysProps().getBoolean(HadoopSecurityManager.OBTAIN_BINARY_TOKEN, false);

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
      userToProxy = getJobProps().getString(HadoopSecurityManager.USER_TO_PROXY);
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

    // TODO: check to see if correct
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
    ArrayList<String> argList = new ArrayList<String>();

    argList.add("--" + MASTER);
    argList.add(jobProps.getString(MASTER, MASTER_DEFAULT));

    logger.info("jars: " + jobProps.getString(SPARK_JARS, SPARK_JARS_DEFAULT));
    System.out.println("jars: " + jobProps.getString(SPARK_JARS, SPARK_JARS_DEFAULT));

    String jarList = HadoopJobUtils.resolveWildCardForJarSpec(getWorkingDirectory(),
            jobProps.getString(SPARK_JARS, SPARK_JARS_DEFAULT), getLog());
    if (jarList != null && jarList.length() > 0) {
      argList.add("--" + SPARK_JARS);
      argList.add(jarList);
    }
    argList.add("--" + EXECUTION_CLASS_SPARK);
    argList.add(jobProps.getString(EXECUTION_CLASS_AZKABAN));
    argList.add("--" + NUM_EXECUTORS_SPARK);
    argList.add(Integer.toString(jobProps.getInt(NUM_EXECUTORS_AZKABAN, NUM_EXECUTORS_DEFAULT)));
    argList.add("--" + EXECUTOR_CORES_SPARK);
    argList.add(Integer.toString(jobProps.getInt(EXECUTOR_CORES_AZKABAN, EXECUTOR_CORES_DEFAULT)));
    argList.add("--" + QUEUE);
    argList.add(jobProps.getString(QUEUE, QUEUE_DEFAULT));
    argList.add("--" + DRIVER_MEMORY_SPARK);
    argList.add(jobProps.getString(DRIVER_MEMORY_AZKABAN, DRIVER_MEMORY_DEFAULT));
    argList.add("--" + EXECUTOR_MEMORY_SPARK);
    argList.add(jobProps.getString(EXECUTOR_MEMORY_AZKABAN, EXECUTOR_MEMORY_DEFAULT));
    for (Entry<String, String> entry : jobProps.getMapByPrefix(SPARK_CONF_AZKABAN).entrySet()) {
      argList.add("--" + SPARK_CONF_SPARK);
      String sparkConfKeyVal = String.format("%s=%s", entry.getKey(), entry.getValue());
      argList.add(sparkConfKeyVal);
    }
    String executionJarName = HadoopJobUtils.resolveExecutionJarName(getWorkingDirectory(),
            jobProps.getString(EXECUTION_JAR_AZKABAN), getLog());
    argList.add(executionJarName);
    argList.add(jobProps.getString(PARAMS, ""));

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
