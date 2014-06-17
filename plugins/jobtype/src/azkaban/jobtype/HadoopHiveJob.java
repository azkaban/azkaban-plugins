/*
 * Copyright 2014 LinkedIn Corp.
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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;

import azkaban.jobExecutor.JavaProcessJob;
import azkaban.utils.Props;
import azkaban.utils.StringUtils;

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

public class HadoopHiveJob extends JavaProcessJob {

  public static final String HIVE_SCRIPT = "hive.script";
  private static final String HIVE_PARAM_PREFIX = "hiveconf.";
  public static final String HADOOP_SECURE_HIVE_WRAPPER =
      "azkaban.jobtype.HadoopSecureHiveWrapper";

  private String userToProxy = null;
  private boolean shouldProxy = false;
  private boolean obtainTokens = false;

  private HadoopSecurityManager hadoopSecurityManager;

  private static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM =
      "hadoop.security.manager.class";

  private boolean debug = false;

  public HadoopHiveJob(String jobid, Props sysProps, Props jobProps, Logger log)
      throws IOException {
    super(jobid, sysProps, jobProps, log);

    getJobProps().put("azkaban.job.id", jobid);

    shouldProxy = getSysProps().getBoolean("azkaban.should.proxy", false);
    getJobProps().put("azkaban.should.proxy", Boolean.toString(shouldProxy));
    obtainTokens = getSysProps().getBoolean("obtain.binary.token", false);

    debug = getJobProps().getBoolean("debug", false);

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
    File tokenFile = null;
    if (shouldProxy && obtainTokens) {
      userToProxy = getJobProps().getString("user.to.proxy");
      getLog().info("Need to proxy. Getting tokens.");
      // get tokens in to a file, and put the location in props
      Props props = new Props();
      props.putAll(getJobProps());
      props.putAll(getSysProps());
      tokenFile = getHadoopTokens(props);
      getJobProps().put("env." + HADOOP_TOKEN_FILE_LOCATION,
          tokenFile.getAbsolutePath());
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

  private HadoopSecurityManager loadHadoopSecurityManager(Props props,
      Logger logger) throws RuntimeException {

    Class<?> hadoopSecurityManagerClass =
        props.getClass(HADOOP_SECURITY_MANAGER_CLASS_PARAM, true,
            HadoopHiveJob.class.getClassLoader());
    getLog().info(
        "Loading hadoop security manager "
            + hadoopSecurityManagerClass.getName());
    HadoopSecurityManager hadoopSecurityManager = null;

    try {
      Method getInstanceMethod =
          hadoopSecurityManagerClass.getMethod("getInstance", Props.class);
      hadoopSecurityManager =
          (HadoopSecurityManager) getInstanceMethod.invoke(
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

  protected File getHadoopTokens(Props props)
      throws HadoopSecurityManagerException {

    File tokenFile = null;
    try {
      tokenFile = File.createTempFile("mr-azkaban", ".token");
    } catch (Exception e) {
      e.printStackTrace();
      throw new HadoopSecurityManagerException(
          "Failed to create the token file.", e);
    }

    hadoopSecurityManager.prefetchToken(tokenFile, props, getLog());

    return tokenFile;
  }

  @Override
  protected String getJavaClass() {
    return HADOOP_SECURE_HIVE_WRAPPER;
  }

  @Override
  protected String getJVMArguments() {
    String args = super.getJVMArguments();

    String typeUserGlobalJVMArgs =
        getJobProps().getString("jobtype.global.jvm.args", null);
    if (typeUserGlobalJVMArgs != null) {
      args += " " + typeUserGlobalJVMArgs;
    }
    String typeSysGlobalJVMArgs =
        getSysProps().getString("jobtype.global.jvm.args", null);
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
    ArrayList<String> list = new ArrayList<String>();

    Map<String, String> map = getHiveConf();
    if (map != null) {
      for (Map.Entry<String, String> entry : map.entrySet()) {
        list.add("-hiveconf "
            + StringUtils.shellQuote(entry.getKey() + "=" + entry.getValue(),
                StringUtils.SINGLE_QUOTE));
      }
    }

    if (debug) {
      list.add("-hiveconf");
      list.add("hive.root.logger=INFO,console");
    }

    list.add("-f");
    list.add(getScript());

    return StringUtils.join((Collection<String>) list, " ");
  }

  @Override
  protected List<String> getClassPaths() {

    List<String> classPath = super.getClassPaths();

    classPath.add(getSourcePathFromClass(Props.class));
    classPath.add(getSourcePathFromClass(HadoopSecureHiveWrapper.class));
    classPath.add(getSourcePathFromClass(HadoopSecurityManager.class));

    List<String> typeClassPath =
        getSysProps().getStringList("jobtype.classpath", null, ",");
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

    List<String> typeGlobalClassPath =
        getSysProps().getStringList("jobtype.global.classpath", null, ",");
    if (typeGlobalClassPath != null) {
      for (String jar : typeGlobalClassPath) {
        if (!classPath.contains(jar)) {
          classPath.add(jar);
        }
      }
    }

    return classPath;
  }

  protected String getScript() {
    return getJobProps().getString(HIVE_SCRIPT);
  }

  protected Map<String, String> getHiveConf() {
    return getJobProps().getMapByPrefix(HIVE_PARAM_PREFIX);
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
}
