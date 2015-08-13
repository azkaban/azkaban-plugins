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

import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Logger;

import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;
import azkaban.utils.Props;

/**
 * There are many common methods that's required by the Hadoop*Job.java's. They are all consolidated
 * here.
 * 
 * @see azkaban.jobtype.HadoopSparkJob
 * @see azkaban.jobtype.HadoopHiveJob
 * @see azkaban.jobtype.HadoopPigJob
 * @see azkaban.jobtype.HadoopJavaJob
 */

public class HadoopJobUtils {

  // FileNameFilter for only jar files
  private static FilenameFilter jarFilter = new FilenameFilter() {
    @Override
    public boolean accept(File dir, String name) {
      if (name.endsWith(".jar"))
        return true;
      else
        return false;
    }
  };

  public static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM = "hadoop.security.manager.class";

  public static void cancelHadoopTokens(HadoopSecurityManager hadoopSecurityManager,
          String userToProxy, File tokenFile, Logger log) {
    try {
      hadoopSecurityManager.cancelTokens(tokenFile, userToProxy, log);
    } catch (HadoopSecurityManagerException e) {
      e.printStackTrace();
      log.error(e.getCause() + e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      log.error(e.getCause() + e.getMessage());
    }
  }

  public static HadoopSecurityManager loadHadoopSecurityManager(Props props, Logger log)
          throws RuntimeException {

    Class<?> hadoopSecurityManagerClass = props.getClass(HADOOP_SECURITY_MANAGER_CLASS_PARAM, true,
            HadoopJobUtils.class.getClassLoader());
    log.info("Loading hadoop security manager " + hadoopSecurityManagerClass.getName());
    HadoopSecurityManager hadoopSecurityManager = null;

    try {
      Method getInstanceMethod = hadoopSecurityManagerClass.getMethod("getInstance", Props.class);
      hadoopSecurityManager = (HadoopSecurityManager) getInstanceMethod.invoke(
              hadoopSecurityManagerClass, props);
    } catch (InvocationTargetException e) {
      log.error("Could not instantiate Hadoop Security Manager "
              + hadoopSecurityManagerClass.getName() + e.getCause());
      throw new RuntimeException(e.getCause());
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e.getCause());
    }

    return hadoopSecurityManager;

  }

  public static File getHadoopTokens(HadoopSecurityManager hadoopSecurityManager, Props props,
          Logger log) throws HadoopSecurityManagerException {

    File tokenFile = null;
    try {
      tokenFile = File.createTempFile("mr-azkaban", ".token");
    } catch (Exception e) {
      e.printStackTrace();
      throw new HadoopSecurityManagerException("Failed to create the token file.", e);
    }

    hadoopSecurityManager.prefetchToken(tokenFile, props, log);

    return tokenFile;
  }

  /**
   * <pre>
   * If there's a * specification in the "jar" argument (e.g. jar=./lib/*,./lib2/*),
   * this method helps to resolve the * into actual jar names inside the folder, and in order.
   * This is due to the requirement that Spark 14 doesn't seem to do the resolution for users
   * 
   * TODO: test case required: one replacement, two replacement.
   * </pre>
   * 
   * @param unresolvedJarSpec
   * @return jar file list, comma separated, all .../* expanded into actual jar names in order
   * 
   */
  public static String resolveWildCardForJarSpec(String workingDirectory, String unresolvedJarSpec,
          Logger log) {

    log.debug("resolveWildCardForJarSpec: unresolved jar specification: " + unresolvedJarSpec);

    if (unresolvedJarSpec == null || unresolvedJarSpec.isEmpty())
      return null;

    StringBuilder resolvedJarSpec = new StringBuilder();

    String[] unresolvedJarSpecList = unresolvedJarSpec.split(",");
    for (String s : unresolvedJarSpecList) {
      // if need resolution
      if (s.endsWith("*")) {
        String fileName = String.format("%s/%s", workingDirectory, s.substring(0, s.length() - 2));
        String[] jarList = new File(fileName).list(jarFilter);
        for (String ls : jarList) {
          String lsFilename = fileName + "/" + ls + ",";
          resolvedJarSpec.append(lsFilename);
        }
      } else { // no need for resolution
        resolvedJarSpec.append(s + ",");
      }
    }

    log.debug("resolveWildCardForJarSpec: resolvedJarSpec: " + resolvedJarSpec);

    // remove the trailing comma
    int lastCharIndex = resolvedJarSpec.length() - 1;
    if (lastCharIndex >= 0 && resolvedJarSpec.charAt(lastCharIndex) == ',') {
      resolvedJarSpec.deleteCharAt(lastCharIndex);
    }

    return resolvedJarSpec.toString();
  }

  /**
   * <pre>
   * This method looks for the proper user execution jar.  
   * The user input is expected in the following 2 formats:
   *   1. ./lib/abc
   *   2. ./lib/abc.jar
   * 
   * This method will use prefix matching to find any jar that is the form of abc*.jar,
   * so that users can bump jar versions without doing modifications to their Hadoop DSL.
   * 
   * This method will throw an Exception if more than one jar that matches the prefix is found
   * 
   * @param workingDirectory
   * @param userSpecifiedJarName
   * @return the resolved actual jar file name to execute
   */
  public static String resolveExecutionJarName(String workingDirectory, String userSpecifiedJarName,
          Logger log) {

    log.debug("Resolving execution jar name: working directory: " + workingDirectory
            + ", user specified name: " + userSpecifiedJarName);

    // in case user decides to specify with abc.jar, instead of only abc
    if (userSpecifiedJarName.endsWith(".jar"))
      userSpecifiedJarName = userSpecifiedJarName.replace(".jar", "");

    // can't use java 1.7 stuff, reverting to a slightly ugly implementation
    String userSpecifiedJarPath = String.format("%s/%s", workingDirectory, userSpecifiedJarName);
    int lastIndexOfSlash = userSpecifiedJarPath.lastIndexOf("/");
    final String jarname = userSpecifiedJarPath.substring(lastIndexOfSlash + 1);
    final String dirName = userSpecifiedJarPath.substring(0, lastIndexOfSlash);
    log.debug("Resolving execution jar name: dirname: " + dirName + ", jar name:  " + jarname);
    File[] potentialExecutionJarList = new File(dirName).listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name.startsWith(jarname.toString()))
          return true;
        else
          return false;
      }
    });

    if (potentialExecutionJarList.length == 0) {
      throw new IllegalStateException("unable to find execution jar for Spark at path: "
              + userSpecifiedJarPath);
    } else if (potentialExecutionJarList.length > 1) {
      throw new IllegalStateException(
              "I find more than one matching instance of the execution jar at the path, don't know which one to use: "
                      + userSpecifiedJarPath);
    }

    String resolvedJarName = potentialExecutionJarList[0].toString();
    log.debug("Resolving execution jar name: resolvedJarName: " + resolvedJarName);
    return resolvedJarName;
  }

  /**
   * <pre>
   * Uses YarnClient to kill the job on HDFS.
   * Using JobClient only works partially:
   *   If yarn container has started but spark job haven't, it will kill
   *   If spark job has started, the cancel will hang until the spark job is complete
   *   If the spark job is complete, it will return immediately, with a job not found on job tracker
   * </pre>
   * 
   * @param applicationId
   */
  public static void killJobOnCluster(String applicationId, Logger log) {
    try {
      YarnConfiguration yarnConf = new YarnConfiguration();
      YarnClient yarnClient = YarnClient.createYarnClient();
      yarnClient.init(yarnConf);
      yarnClient.start();

      String[] split = applicationId.split("_");
      ApplicationId aid = ApplicationId.newInstance(Long.parseLong(split[1]),
              Integer.parseInt(split[2]));

      log.debug("start klling applicatin: " + aid);
      yarnClient.killApplication(aid);
      log.debug("successfully killed application: " + aid);
    } catch (Exception e) {
      log.error("Failure while try to kill the hadoop job.  Will skip and continue", e);
    }
  }

}
