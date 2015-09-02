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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  public static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM = "hadoop.security.manager.class";

  public static final String APPLICATION_ID_REGEX = ".* (application_\\d+_\\d+).*";

  public static String JOBTYPE_GLOBAL_JVM_ARGS = "jobtype.global.jvm.args";

  public static String JOBTYPE_JVM_ARGS = "jobtype.jvm.args";

  public static String JVM_ARGS = "jvm.args";

  public static void cancelHadoopTokens(HadoopSecurityManager hadoopSecurityManager,
          String userToProxy, File tokenFile, Logger log) {
    try {
      hadoopSecurityManager.cancelTokens(tokenFile, userToProxy, log);
    } catch (HadoopSecurityManagerException e) {
      log.error(e.getCause() + e.getMessage());
    } catch (Exception e) {
      log.error(e.getCause() + e.getMessage());
    }
  }

  /**
   * Based on the HADOOP_SECURITY_MANAGER_CLASS_PARAM setting in the incoming props, finds the
   * correct HadoopSecurityManager Java class
   * 
   * @param props
   * @param log
   * @return a HadoopSecurityManager object. Will throw exception if any errors occur (including not
   *         finding a class)
   * @throws RuntimeException
   *           : If any errors happen along the way.
   */
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
      throw new RuntimeException(e.getCause());
    }

    return hadoopSecurityManager;

  }

  /**
   * Fetching token with the Azkaban user
   * 
   * @param hadoopSecurityManager
   * @param props
   * @param log
   * @return
   * @throws HadoopSecurityManagerException
   */
  public static File getHadoopTokens(HadoopSecurityManager hadoopSecurityManager, Props props,
          Logger log) throws HadoopSecurityManagerException {

    File tokenFile = null;
    try {
      tokenFile = File.createTempFile("mr-azkaban", ".token");
    } catch (Exception e) {
      throw new HadoopSecurityManagerException("Failed to create the token file.", e);
    }

    hadoopSecurityManager.prefetchToken(tokenFile, props, log);

    return tokenFile;
  }

  /**
   * <pre>
   * If there's a * specification in the "jar" argument (e.g. jar=./lib/*,./lib2/*),
   * this method helps to resolve the * into actual jar names inside the folder, and in order.
   * This is due to the requirement that Spark 1.4 doesn't seem to do the resolution for users
   * 
   * </pre>
   * 
   * @param unresolvedJarSpec
   * @return jar file list, comma separated, all .../* expanded into actual jar names in order
   * 
   */
  public static String resolveWildCardForJarSpec(String workingDirectory, String unresolvedJarSpec,
          Logger log) {

    log.debug("resolveWildCardForJarSpec: unresolved jar specification: " + unresolvedJarSpec);
    log.debug("working directory: " + workingDirectory);

    if (unresolvedJarSpec == null || unresolvedJarSpec.isEmpty())
      return "";

    StringBuilder resolvedJarSpec = new StringBuilder();

    String[] unresolvedJarSpecList = unresolvedJarSpec.split(",");
    for (String s : unresolvedJarSpecList) {
      // if need resolution
      if (s.endsWith("*")) {
        // remove last 2 characters to get to the folder
        String dirName = String.format("%s/%s", workingDirectory, s.substring(0, s.length() - 2));

        File[] jars = null;
        try {
          jars = getFilesInFolderByRegex(new File(dirName), ".*jar");
        } catch (FileNotFoundException fnfe) {
          log.warn("folder does not exist: " + dirName);
          continue;
        }

        // if the folder is there, add them to the jar list
        for (File jar : jars) {
          resolvedJarSpec.append(jar.toString() + ",");
        }
      } else { // no need for resolution
        resolvedJarSpec.append(s).append(",");
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
  public static String resolveExecutionJarName(String workingDirectory,
          String userSpecifiedJarName, Logger log) {

    log.debug("Resolving execution jar name: working directory: " + workingDirectory
            + ", user specified name: " + userSpecifiedJarName);

    // in case user decides to specify with abc.jar, instead of only abc
    if (userSpecifiedJarName.endsWith(".jar"))
      userSpecifiedJarName = userSpecifiedJarName.replace(".jar", "");

    // can't use java 1.7 stuff, reverting to a slightly ugly implementation
    String userSpecifiedJarPath = String.format("%s/%s", workingDirectory, userSpecifiedJarName);
    int lastIndexOfSlash = userSpecifiedJarPath.lastIndexOf("/");
    final String jarPrefix = userSpecifiedJarPath.substring(lastIndexOfSlash + 1);
    final String dirName = userSpecifiedJarPath.substring(0, lastIndexOfSlash);
    log.debug("Resolving execution jar name: dirname: " + dirName + ", jar name:  " + jarPrefix);

    File[] potentialExecutionJarList;
    try {
      potentialExecutionJarList = getFilesInFolderByRegex(new File(dirName), jarPrefix + ".*jar");
    } catch (FileNotFoundException e) {
      throw new IllegalStateException(
              "execution jar is suppose to be in this folder, but the folder doesn't exist: "
                      + dirName);
    }

    if (potentialExecutionJarList.length == 0) {
      throw new IllegalStateException("unable to find execution jar for Spark at path: "
              + userSpecifiedJarPath + "*.jar");
    } else if (potentialExecutionJarList.length > 1) {
      throw new IllegalStateException(
              "I find more than one matching instance of the execution jar at the path, don't know which one to use: "
                      + userSpecifiedJarPath + "*.jar");
    }

    String resolvedJarName = potentialExecutionJarList[0].toString();
    log.debug("Resolving execution jar name: resolvedJarName: " + resolvedJarName);
    return resolvedJarName;
  }

  /**
   * 
   * @return a list of files in the given folder that matches the regex. It may be empty, but will
   *         never return a null
   * @throws FileNotFoundException
   */
  private static File[] getFilesInFolderByRegex(File folder, final String regex)
          throws FileNotFoundException {
    // sanity check

    if (!folder.exists()) {
      throw new FileNotFoundException();

    }
    if (!folder.isDirectory()) {
      throw new IllegalStateException(
              "execution jar is suppose to be in this folder, but the object present is not a directory: "
                      + folder);
    }

    File[] matchingFiles = folder.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name.matches(regex))
          return true;
        else
          return false;
      }
    });

    if (matchingFiles == null) {
      throw new IllegalStateException(
              "the ls command in ResolveExecutionJarName threw an IOException");
    }

    return matchingFiles;
  }

  /**
   * Pass in a log file, this method will find all the hadoop jobs it has launched, and kills it
   * 
   * Only works with Hadoop2
   * 
   * @param logFilePath
   * @param log
   * @return a Set<String>. The set will contain the applicationIds that this job tried to kill.
   */
  public static Set<String> killAllSpawnedHadoopJobs(String logFilePath, Logger log) {
    Set<String> allSpawnedJobs = findApplicationIdFromLog(logFilePath, log);
    log.info("applicationIds to kill: " + allSpawnedJobs);

    for (String appId : allSpawnedJobs) {
      try {
        killJobOnCluster(appId, log);
      } catch (Throwable t) {
        log.warn("something happened while trying to kill this job: " + appId, t);
      }
    }

    return allSpawnedJobs;
  }

  /**
   * <pre>
   * Takes in a log file, will grep every line to look for the application_id pattern.
   * If it finds multiple, it will return all of them, de-duped (this is possible in the case of pig jobs)
   * This can be used in conjunction with the @killJobOnCluster method in this file.
   * </pre>
   * 
   * @param logFilePath
   * @return a Set. May be empty, but will never be null
   */
  public static Set<String> findApplicationIdFromLog(String logFilePath, Logger log) {

    File logFile = new File(logFilePath);

    if (!logFile.exists()) {
      throw new IllegalArgumentException("the logFilePath does not exist: " + logFilePath);
    }
    if (!logFile.isFile()) {
      throw new IllegalArgumentException("the logFilePath specified  is not a valid file: "
              + logFilePath);
    }
    if (!logFile.canRead()) {
      throw new IllegalArgumentException("unable to read the logFilePath specified: " + logFilePath);
    }

    BufferedReader br = null;
    Set<String> applicationIds = new HashSet<String>();
    Pattern p = Pattern.compile(APPLICATION_ID_REGEX);

    try {
      br = new BufferedReader(new FileReader(logFile));
      String input;

      // finds all the application IDs
      while ((input = br.readLine()) != null) {
        Matcher m = p.matcher(input);
        if (m.find()) {
          String appId = m.group(1);
          if (!applicationIds.contains(appId)) {
            applicationIds.add(appId);
          }
        }
      }
    } catch (IOException e) {
      log.error("Error while trying to find applicationId for Spark log", e);
    } finally {
      try {
        if (br != null)
          br.close();
      } catch (Exception e) {
        // do nothing
      }
    }
    return applicationIds;
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

      log.info("start klling application: " + aid);
      yarnClient.killApplication(aid);
      log.info("successfully killed application: " + aid);
    } catch (Exception e) {
      log.error("Failure while try to kill the hadoop job.  Will skip and continue", e);
    }
  }

  /**
   * <pre>
   * constructions a javaOpts string based on the Props, and the key given, will return 
   *  String.format("-D%s=%s", key, value);
   * </pre>
   * 
   * @param props
   * @param key
   * @return will return String.format("-D%s=%s", key, value). Throws RuntimeException if props not
   *         present
   */
  public static String javaOptStringFromAzkabanProps(Props props, String key) {
    String value = props.get(key);
    if (value == null) {
      throw new RuntimeException(String.format("Cannot find property [%s], in azkaban props: [%s]",
              key, value));
    }
    return String.format("-D%s=%s", key, value);
  }
}
