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

import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import azkaban.utils.Props;

/**
 * <pre>
 * A Spark wrapper (more specifically a spark-submit wrapper) that works with Azkaban.
 * This class will receive input from {@link azkaban.jobtype.HadoopSparkJob}, and pass it on to spark-submit
 * </pre>
 *
 * @see azkaban.jobtype.HadoopSecureSparkWrapper
 */
public class HadoopSecureSparkWrapper {

  private static boolean securityEnabled;

  private static final Logger logger = Logger.getRootLogger();

  /**
   * Entry point: a Java wrapper to the spark-submit command
   * 
   * @param args
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {

    Properties jobProps = HadoopSecureWrapperUtils.loadAzkabanProps();
    HadoopConfigurationInjector.injectResources(new Props(null, jobProps));

    // set up hadoop related configurations
    final Configuration conf = new Configuration();
    UserGroupInformation.setConfiguration(conf);
    securityEnabled = UserGroupInformation.isSecurityEnabled();

    if (HadoopSecureWrapperUtils.shouldProxy(jobProps)) {
      UserGroupInformation proxyUser = null;
      String userToProxy = jobProps.getProperty("user.to.proxy");
      if (securityEnabled) {
        proxyUser = HadoopSecureWrapperUtils.createSecurityEnabledProxyUser(userToProxy, logger);
      } else {
        proxyUser = UserGroupInformation.createRemoteUser(userToProxy);
      }
      logger.info("Proxying to execute job.  Proxied as user " + userToProxy);

      proxyUser.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          runSpark(args);
          return null;
        }
      });

    } else {
      logger.info("Not proxying to execute job. ");
      runSpark(args);
    }
  }

  /**
   * Actually calling the spark-submit command
   * 
   * @param args
   */
  private static void runSpark(String[] args) {

    if (args.length == 0) {
      throw new RuntimeException("SparkSubmit cannot run with zero args");
    }

    // munge everything together and repartition based by our ^Z character, instead of by the
    // default "space" character
    StringBuilder concat = new StringBuilder();
    concat.append(args[0]);
    for (int i = 1; i < args.length; i++) {
      concat.append(" " + args[i]);
    }

    final String[] newArgs = concat.toString().split(SparkJobArg.delimiter);
    logger.info("newArgs: " + Arrays.toString(newArgs));

    org.apache.spark.deploy.SparkSubmit$.MODULE$.main(newArgs);
  }

}
