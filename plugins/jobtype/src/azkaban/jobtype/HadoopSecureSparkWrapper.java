/*
 * Copyright 2015 LinkedIn Corp.
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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;

import azkaban.jobExecutor.ProcessJob;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;

public class HadoopSecureSparkWrapper {

  // param values
  private static boolean securityEnabled;

  private static final Logger logger = Logger.getRootLogger();

  /**
   * Entry point: a Java wrapper to the spark-submit command
   * 
   * @param args
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {

    Properties jobProps = loadCombinedProps();
    HadoopConfigurationInjector.injectResources(new Props(null, jobProps));

    // set up hadoop related configurations
    final Configuration conf = new Configuration();
    UserGroupInformation.setConfiguration(conf);
    securityEnabled = UserGroupInformation.isSecurityEnabled();

    if (shouldProxy(jobProps)) {
      UserGroupInformation proxyUser = null;
      String userToProxy = jobProps.getProperty("user.to.proxy");
      if (securityEnabled) {
        proxyUser = createSecurityEnabledProxyUser(userToProxy);
      } else {
        proxyUser = UserGroupInformation.createRemoteUser(userToProxy);
      }

      logger.info("Proxied as user " + userToProxy);

      proxyUser.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          runSpark(args);
          return null;
        }
      });

    } else {
      logger.info("Not proxying. ");
      runSpark(args);
    }
  }

  /**
   * Actually calling the spark-submit command
   * 
   * @param args
   */
  private static void runSpark(String[] args) {

    logger.info("args: " + Arrays.toString(args));

    org.apache.spark.deploy.SparkSubmit$.MODULE$.main(args);
  }

  /**
   * Perform all the magic required to get the proxyUser in a securitized grid
   * 
   * @param userToProxy
   * @return
   * @throws IOException
   */
  private static UserGroupInformation createSecurityEnabledProxyUser(String userToProxy)
          throws IOException {
    UserGroupInformation proxyUser;
    String filelocation = System.getenv(HADOOP_TOKEN_FILE_LOCATION);
    if (filelocation == null) {
      throw new RuntimeException("hadoop token information not set.");
    }
    if (!new File(filelocation).exists()) {
      throw new RuntimeException("hadoop token file doesn't exist.");
    }

    logger.info("Found token file " + filelocation);

    logger.info("Setting " + HadoopSecurityManager.MAPREDUCE_JOB_CREDENTIALS_BINARY + " to "
            + filelocation);
    System.setProperty(HadoopSecurityManager.MAPREDUCE_JOB_CREDENTIALS_BINARY, filelocation);

    UserGroupInformation loginUser = null;

    loginUser = UserGroupInformation.getLoginUser();
    logger.info("Current logged in user is " + loginUser.getUserName());

    logger.info("Creating proxy user.");
    proxyUser = UserGroupInformation.createProxyUser(userToProxy, loginUser);

    for (Token<?> token : loginUser.getTokens()) {
      proxyUser.addToken(token);
    }
    return proxyUser;
  }

  /**
   * Loading the properties file, which is a combination of the jobProps file and sysProps file
   * 
   * @return
   * @throws IOException
   * @throws FileNotFoundException
   */
  private static Properties loadCombinedProps() throws IOException, FileNotFoundException {
    String propsFile = System.getenv(ProcessJob.JOB_PROP_ENV);
    Properties props = new Properties();
    props.load(new BufferedReader(new FileReader(propsFile)));
    return props;
  }

  private static boolean shouldProxy(Properties props) {
    String shouldProxy = props.getProperty(HadoopSecurityManager.ENABLE_PROXYING);

    return shouldProxy != null && shouldProxy.equals("true");
  }

}
