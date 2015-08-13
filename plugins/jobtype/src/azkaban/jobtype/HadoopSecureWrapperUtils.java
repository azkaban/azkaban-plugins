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
import java.util.Properties;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;

import azkaban.jobExecutor.ProcessJob;
import azkaban.security.commons.HadoopSecurityManager;

/**
 * <pre>
 * There are many common methods that's required by the HadoopSecure*Wrapper.java's. They are all consolidated
 * here.
 * </pre>
 *
 * @see azkaban.jobtype.HadoopSecurePigWrapper
 * @see azkaban.jobtype.HadoopSecureHiveWrapper
 * @see azkaban.jobtype.HadoopSecureSparkWrapper
 */
public class HadoopSecureWrapperUtils {

  /**
   * Perform all the magic required to get the proxyUser in a securitized grid
   * 
   * @param userToProxy
   * @return
   * @throws IOException
   */
  public static UserGroupInformation createSecurityEnabledProxyUser(String userToProxy, Logger log)
          throws IOException {

    String filelocation = System.getenv(HADOOP_TOKEN_FILE_LOCATION);
    if (filelocation == null) {
      throw new RuntimeException("hadoop token information not set.");
    }
    if (!new File(filelocation).exists()) {
      throw new RuntimeException("hadoop token file doesn't exist.");
    }

    log.info("Found token file " + filelocation);

    log.info("Setting " + HadoopSecurityManager.MAPREDUCE_JOB_CREDENTIALS_BINARY + " to "
            + filelocation);
    System.setProperty(HadoopSecurityManager.MAPREDUCE_JOB_CREDENTIALS_BINARY, filelocation);

    UserGroupInformation loginUser = null;

    loginUser = UserGroupInformation.getLoginUser();
    log.info("Current logged in user is " + loginUser.getUserName());

    log.info("Creating proxy user.");
    UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(userToProxy, loginUser);

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
  public static Properties loadAzkabanProps() throws IOException, FileNotFoundException {
    String propsFile = System.getenv(ProcessJob.JOB_PROP_ENV);
    Properties props = new Properties();
    props.load(new BufferedReader(new FileReader(propsFile)));
    return props;
  }

  /**
   * Looks for particular properties inside the Properties object passed in, and determines whether
   * proxying should happen or not
   * 
   * @param props
   * @return
   */
  public static boolean shouldProxy(Properties props) {
    String shouldProxy = props.getProperty(HadoopSecurityManager.ENABLE_PROXYING);
    return shouldProxy != null && shouldProxy.equals("true");
  }

}
