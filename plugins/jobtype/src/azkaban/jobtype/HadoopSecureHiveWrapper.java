/*
 * Copyright 2012 LinkedIn, Inc
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

import static azkaban.security.commons.SecurityUtils.MAPREDUCE_JOB_CREDENTIALS_BINARY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEAUXJARS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORECONNECTURLKEY;
import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.cli.OptionsProcessor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;

import azkaban.jobExecutor.ProcessJob;
import azkaban.jobtype.hiveutils.HiveQueryExecutionException;
import azkaban.security.commons.HadoopSecurityManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

public class HadoopSecureHiveWrapper {

  private static boolean securityEnabled;
  private static final Logger logger = Logger.getRootLogger();

  private static CliSessionState ss;
  private static String hiveScript;

  public static void main(final String[] args) throws Exception {

    String propsFile = System.getenv(ProcessJob.JOB_PROP_ENV);
    Properties prop = new Properties();
    prop.load(new BufferedReader(new FileReader(propsFile)));

    hiveScript = prop.getProperty("hive.script");

    final Configuration conf = new Configuration();

    UserGroupInformation.setConfiguration(conf);
    securityEnabled = UserGroupInformation.isSecurityEnabled();

    if (shouldProxy(prop)) {
      UserGroupInformation proxyUser = null;
      String userToProxy = prop.getProperty("user.to.proxy");
      if (securityEnabled) {
        String filelocation = System.getenv(HADOOP_TOKEN_FILE_LOCATION);
        if (filelocation == null) {
          throw new RuntimeException("hadoop token information not set.");
        }
        if (!new File(filelocation).exists()) {
          throw new RuntimeException("hadoop token file doesn't exist.");
        }

        logger.info("Found token file " + filelocation);

        logger.info("Setting "
            + HadoopSecurityManager.MAPREDUCE_JOB_CREDENTIALS_BINARY + " to "
            + filelocation);
        System.setProperty(
            HadoopSecurityManager.MAPREDUCE_JOB_CREDENTIALS_BINARY,
            filelocation);

        UserGroupInformation loginUser = null;

        loginUser = UserGroupInformation.getLoginUser();
        logger.info("Current logged in user is " + loginUser.getUserName());

        logger.info("Creating proxy user.");
        proxyUser =
            UserGroupInformation.createProxyUser(userToProxy, loginUser);

        for (Token<?> token : loginUser.getTokens()) {
          proxyUser.addToken(token);
        }
      } else {
        proxyUser = UserGroupInformation.createRemoteUser(userToProxy);
      }

      logger.info("Proxied as user " + userToProxy);

      proxyUser.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          runHive(args);
          return null;
        }
      });

    } else {
      logger.info("Not proxying. ");
      runHive(args);
    }
  }

  public static void runHive(String[] args) throws Exception {

    final HiveConf hiveConf = new HiveConf(SessionState.class);

    if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
      System.out.println("Setting hadoop tokens ... ");
      hiveConf.set(MAPREDUCE_JOB_CREDENTIALS_BINARY,
          System.getenv(HADOOP_TOKEN_FILE_LOCATION));
      System.setProperty(MAPREDUCE_JOB_CREDENTIALS_BINARY,
          System.getenv(HADOOP_TOKEN_FILE_LOCATION));
    }

    logger.info("HiveConf = " + hiveConf);
    logger.info("According to the conf, we're talking to the Hive hosted at: "
        + HiveConf.getVar(hiveConf, METASTORECONNECTURLKEY));

    String orig = HiveConf.getVar(hiveConf, HIVEAUXJARS);
    String expanded = expandHiveAuxJarsPath(orig);
    if (orig == null || orig.equals(expanded)) {
      logger.info("Hive aux jars variable not expanded");
    } else {
      logger.info("Expanded aux jars variable from [" + orig + "] to ["
          + expanded + "]");
      HiveConf.setVar(hiveConf, HIVEAUXJARS, expanded);
    }

    OptionsProcessor op = new OptionsProcessor();

    if (!op.process_stage1(new String[] {})) {
      throw new IllegalArgumentException("Can't process empty args?!?");
    }

    if (!ShimLoader.getHadoopShims().usesJobShell()) {
      // hadoop-20 and above - we need to augment classpath using hiveconf
      // components
      // see also: code in ExecDriver.java
      ClassLoader loader = hiveConf.getClassLoader();
      String auxJars = HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVEAUXJARS);
      logger.info("Got auxJars = " + auxJars);

      if (StringUtils.isNotBlank(auxJars)) {
        loader =
            Utilities.addToClassPath(loader, StringUtils.split(auxJars, ","));
      }
      hiveConf.setClassLoader(loader);
      Thread.currentThread().setContextClassLoader(loader);
    }

    // See https://issues.apache.org/jira/browse/HIVE-1411
    hiveConf.set("datanucleus.plugin.pluginRegistryBundleCheck", "LOG");

    // to force hive to use the jobclient to submit the job, never using
    // HADOOPBIN (to do localmode)
    hiveConf.setBoolean("hive.exec.mode.local.auto", false);

    ss = new CliSessionState(hiveConf);
    SessionState.start(ss);

    logger.info("SessionState = " + ss);
    ss.out = System.out;
    ss.err = System.err;
    ss.in = System.in;

    if (!op.process_stage2(ss)) {
      throw new IllegalArgumentException(
          "Can't process arguments from session state");
    }

    logger.info("Executing query: " + hiveScript);

    CliDriver cli = new CliDriver();
    int returnCode = cli.processFile(hiveScript);
    if (returnCode != 0) {
      logger.warn("Got exception " + returnCode + " from line: " + hiveScript);
      throw new HiveQueryExecutionException(returnCode, hiveScript);
    }
  }

  public static boolean shouldProxy(Properties prop) {
    String shouldProxy =
        prop.getProperty(HadoopSecurityManager.ENABLE_PROXYING);

    return shouldProxy != null && shouldProxy.equals("true");
  }

  /**
   * Normally hive.aux.jars.path is expanded from just being a path to the full
   * list of files in the directory by the hive shell script. Since we normally
   * won't be running from the script, it's up to us to do that work here. We
   * use a heuristic that if there is no occurrence of ".jar" in the original,
   * it needs expansion. Otherwise it's already been done for us.
   *
   * Also, surround the files with uri niceities.
   */
  static String expandHiveAuxJarsPath(String original) throws IOException {
    if (original == null || original.contains(".jar"))
      return original;

    File[] files = new File(original).listFiles();

    if (files == null || files.length == 0) {
      logger
          .info("No files in to expand in aux jar path. Returning original parameter");
      return original;
    }

    return filesToURIString(files);

  }

  static String filesToURIString(File[] files) throws IOException {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < files.length; i++) {
      sb.append("file:///").append(files[i].getCanonicalPath());
      if (i != files.length - 1)
        sb.append(",");
    }

    return sb.toString();
  }

}
