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

import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import azkaban.jobExecutor.JavaProcessJob;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;
import azkaban.utils.Props;
import azkaban.utils.StringUtils;

public class HadoopSparkJob extends JavaProcessJob {

  // Azkaban/Java params
  private static final String USER_TO_PROXY = "user.to.proxy";

  private static final String HADOOP_SECURE_SPARK_WRAPPER = HadoopSecureSparkWrapper.class
          .getName();

  private static final Logger logger = Logger.getRootLogger();

  // Spark params
  private static final String MASTER = "master";

  private static final String JARS_PREFIX = "jars";

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

  // Spark default
  private static final String MASTER_DEFAULT = "yarn-cluster";

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

  private static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM = "hadoop.security.manager.class";

  // FileNameFilter for only jar files
  private static FilenameFilter jarFilter = new FilenameFilter() {
    @Override
    public boolean accept(File dir, String name) {
      if (name.endsWith("\\.jar"))
        return true;
      else
        return false;
    }
  };

  public HadoopSparkJob(String jobid, Props sysProps, Props jobProps, Logger log) {
    super(jobid, sysProps, jobProps, log);

    getJobProps().put("azkaban.job.id", jobid);

    shouldProxy = getSysProps().getBoolean("azkaban.should.proxy", false);
    getJobProps().put("azkaban.should.proxy", Boolean.toString(shouldProxy));
    obtainTokens = getSysProps().getBoolean("obtain.binary.token", false);

    if (shouldProxy) {
      getLog().info("Initiating hadoop security manager.");
      try {
        hadoopSecurityManager = loadHadoopSecurityManager(sysProps, log);
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
      tokenFile = getHadoopTokens(props);
      getJobProps().put("env." + HADOOP_TOKEN_FILE_LOCATION, tokenFile.getAbsolutePath());
    }

    try {
      super.run();
    } catch (Exception e) {
      e.printStackTrace();
      getLog().error("caught exception running the job");
      throw new Exception(e);
    } catch (Throwable t) {
      t.printStackTrace();
      getLog().error("caught error running the job");
      throw new Exception(t);
    } finally {
      if (tokenFile != null) {
        cancelHadoopTokens(tokenFile);
        if (tokenFile.exists()) {
          tokenFile.delete();
        }
      }
    }
  }

  private HadoopSecurityManager loadHadoopSecurityManager(Props props, Logger logger)
          throws RuntimeException {

    Class<?> hadoopSecurityManagerClass = props.getClass(HADOOP_SECURITY_MANAGER_CLASS_PARAM, true,
            HadoopHiveJob.class.getClassLoader());
    getLog().info("Loading hadoop security manager " + hadoopSecurityManagerClass.getName());
    HadoopSecurityManager hadoopSecurityManager = null;

    try {
      Method getInstanceMethod = hadoopSecurityManagerClass.getMethod("getInstance", Props.class);
      hadoopSecurityManager = (HadoopSecurityManager) getInstanceMethod.invoke(
              hadoopSecurityManagerClass, props);
    } catch (InvocationTargetException e) {
      getLog().error(
              "Could not instantiate Hadoop Security Manager "
                      + hadoopSecurityManagerClass.getName() + e.getCause());
      throw new RuntimeException(e.getCause());
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e.getCause());
    }

    return hadoopSecurityManager;

  }

  private void cancelHadoopTokens(File tokenFile) {
    try {
      hadoopSecurityManager.cancelTokens(tokenFile, userToProxy, getLog());
    } catch (HadoopSecurityManagerException e) {
      e.printStackTrace();
      getLog().error(e.getCause() + e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      getLog().error(e.getCause() + e.getMessage());
    }
  }

  protected File getHadoopTokens(Props props) throws HadoopSecurityManagerException {

    File tokenFile = null;
    try {
      tokenFile = File.createTempFile("mr-azkaban", ".token");
    } catch (Exception e) {
      e.printStackTrace();
      throw new HadoopSecurityManagerException("Failed to create the token file.", e);
    }

    hadoopSecurityManager.prefetchToken(tokenFile, props, getLog());

    return tokenFile;
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

    // what happens if I don't add these 2 things?
    // args[0] = "--driver-java-options";
    // args[1] = javaOption(WORKFLOW_LINK) + javaOption(JOB_LINK)
    // + javaOption(EXECUTION_LINK) + javaOption(ATTEMPT_LINK);

    argList.add("--" + MASTER);
    argList.add(jobProps.getString(MASTER, MASTER_DEFAULT));

    logger.info("jars: " + jobProps.getString(JARS_PREFIX, ""));
    System.out.println("jars: " + jobProps.getString(JARS_PREFIX, ""));

    String jarList = resolveJars(jobProps.getString(JARS_PREFIX, ""));
    if (jarList != null && jarList.length() > 0) {
      argList.add("--" + JARS_PREFIX);
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
    // always place EXECUTION_JAR_AZKABAN in the last spot
    argList.add(jobProps.getString(EXECUTION_JAR_AZKABAN));

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
   * Resolves /* and expands them into appropriate jar file names. Only works for jar file as of now
   * (only known use case)
   * 
   * test case required: one replacement, two replacement.
   * 
   * @param unresolvedJar
   * @return jar file list, comma separated, all .../* expanded into actual jar names in order
   */
  private String resolveJars(String unresolvedJar) {

    System.err.println("unResolvedJar: " + unresolvedJar);

    if (unresolvedJar == null || unresolvedJar.isEmpty())
      return null;

    String[] unresolvedJarList = unresolvedJar.split(",");

    StringBuilder resolvedJar = new StringBuilder();
    for (String s : unresolvedJarList) {
      // if need resolution
      System.err.println("currently resolving jar: " + s);
      if (s.endsWith("*")) {
        String fileName = String.format("%s/%s", getWorkingDirectory(),
                s.substring(0, s.length() - 2));
        System.err.println(fileName);
        String[] lsList = new File(fileName).list(jarFilter);
        System.err.println("absolute path: " + new File(fileName).getAbsolutePath());
        System.err.println("ls length: " + lsList.length);
        for (String ls : lsList) {
          String lsFilename = fileName + "/" + ls + ",";
          System.err.println(lsFilename);
          resolvedJar.append(lsFilename);
        }
      } else { // no need for resolution
        resolvedJar.append(s + ",");
      }
    }

    logger.error("resolvedJar: " + resolvedJar);
    System.err.println("resolvedJar: " + resolvedJar);

    // remove the trailing comma
    // TODO need to check length of resolve jar: if zero don't do anything
    int lastCharIndex = resolvedJar.length() - 1;
    if (resolvedJar.charAt(lastCharIndex) == ',') {
      resolvedJar.deleteCharAt(resolvedJar.length() - 1);
    }

    return resolvedJar.toString();
  }

}
