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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;
import static azkaban.flow.CommonJobProperties.ATTEMPT_LINK;
import static azkaban.flow.CommonJobProperties.EXECUTION_LINK;
import static azkaban.flow.CommonJobProperties.JOB_LINK;
import static azkaban.flow.CommonJobProperties.WORKFLOW_LINK;
import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

/**
 * <pre>
 * A Spark wrapper (more specifically a spark-submit wrapper) that works with Azkaban.
 * This class will be running on a separate process with JVM/ENV properties, classpath and main args
 *  built from {@link azkaban.jobtype.HadoopSparkJob}.
 * This class's main() will receive input args built from {@link azkaban.jobtype.HadoopSparkJob},
 *  and pass it on to spark-submit to launch spark job.
 * This process will be the client of the spark job.
 *
 * </pre>
 *
 * @see azkaban.jobtype.HadoopSecureSparkWrapper
 */
public class HadoopSecureSparkWrapper {

  private static final Logger logger = Logger.getRootLogger();
  private static final String EMPTY_STRING = "";

  //SPARK CONF PARAM
  private static final String SPARK_CONF_EXTRA_DRIVER_OPTIONS = "spark.driver.extraJavaOptions";
  private static final String SPARK_CONF_SHUFFLE_SERVICE_ENABLED = "spark.shuffle.service.enabled";
  private static final String SPARK_CONF_DYNAMIC_ALLOC_ENABLED = "spark.dynamicAllocation.enabled";
  private static final String SPARK_CONF_QUEUE = "spark.yarn.queue";

  //YARN CONF PARAM
  private static final String YARN_CONF_NODE_LABELING_ENABLED = "yarn.node-labels.enabled";

  /**
   * Entry point: a Java wrapper to the spark-submit command
   * Args is built in HadoopSparkJob. 
   * 
   * @param args
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {

    Properties jobProps = HadoopSecureWrapperUtils.loadAzkabanProps();
    HadoopConfigurationInjector.injectResources(new Props(null, jobProps));

    if (HadoopSecureWrapperUtils.shouldProxy(jobProps)) {
      String tokenFile = System.getenv(HADOOP_TOKEN_FILE_LOCATION);
      UserGroupInformation proxyUser =
          HadoopSecureWrapperUtils.setupProxyUser(jobProps, tokenFile, logger);
      proxyUser.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          runSpark(args);
          return null;
        }
      });
    } else {
      runSpark(args);
    }   
  }

  /**
   * Actually adjusts cmd args based on execution environment and calls the spark-submit command
   * 
   * @param args
   */
  private static void runSpark(String[] args) {

    if (args.length == 0) {
      throw new RuntimeException("SparkSubmit cannot run with zero args");
    }

    // Arg String passed to here are long strings delimited by SparkJobArg.delimiter
    // munge everything together and repartition based by our ^Z character, instead of by the
    // default "space" character
    StringBuilder concat = new StringBuilder();
    concat.append(args[0]);
    for (int i = 1; i < args.length; i++) {
      concat.append(" " + args[i]);
    }
    String[] newArgs = concat.toString().split(SparkJobArg.delimiter);

    // Sample: [--driver-java-options, , --master, yarn-cluster, --class, myclass,
    // --conf, queue=default, --executor-memory, 1g, --num-executors, 15, my.jar, myparams]
    logger.info("Args before adjusting driver java opts: " + Arrays.toString(newArgs));

    // Adjust driver java opts param
    handleDriverJavaOpts(newArgs);

    // If dynamic allocation policy for this jobtype is turned on, adjust related param
    handleDynamicResourceAllocation(newArgs);

    // If yarn cluster enables node labeling, adjust related param
    handleNodeLabeling(newArgs);

    // Realign params after adjustment
    newArgs = removeNullsFromArgArray(newArgs);
    logger.info("Args after adjusting driver java opts: " + Arrays.toString(newArgs));

    org.apache.spark.deploy.SparkSubmit$.MODULE$.main(newArgs);
  }

  private static void handleDriverJavaOpts(String[] argArray) {
    Configuration conf = new Configuration();
    // Driver java opts is always the first elem(param name) and second elem(value) in the argArray
    // Get current driver java opts here
    StringBuilder driverJavaOptions = new StringBuilder(argArray[1]);
    // In spark-submit, when both --driver-java-options and conf spark.driver.extraJavaOptions is used,
    // spark-submit will only pick --driver-java-options, an arg we always have
    // So if user gives --conf spark.driver.extraJavaOptions=XX, we append the value in --driver-java-options
    for (int i = 0; i < argArray.length; i++) {
      if (argArray[i].equals(SparkJobArg.SPARK_CONF_PREFIX.sparkParamName) 
        && argArray[i+1].startsWith(SPARK_CONF_EXTRA_DRIVER_OPTIONS)) {
        driverJavaOptions.append(argArray[++i].substring(SPARK_CONF_EXTRA_DRIVER_OPTIONS.length() + 1));
      }
    }
    
    // Append addtional driver java opts about azkaban context
    String[] requiredJavaOpts = { WORKFLOW_LINK, JOB_LINK, EXECUTION_LINK, ATTEMPT_LINK };
    for (int i = 0; i < requiredJavaOpts.length; i++) {
        driverJavaOptions.append(" ").append(HadoopJobUtils.javaOptStringFromHadoopConfiguration(conf,
                  requiredJavaOpts[i]));
    }
    // Update driver java opts
    argArray[1] = driverJavaOptions.toString();
  }

  private static void handleDynamicResourceAllocation(String[] argArray) {
    // HadoopSparkJob will set env var on this process if we enable dynamic allocation policy for spark jobtype.
    // This policy can be enabled in spark jobtype plugin's conf property.
    // This policy is designed to enforce dynamic allocation.
    String dynamicAllocProp = System.getenv(HadoopSparkJob.SPARK_DYNAMIC_RES_ENV_VAR);
    boolean dynamicAllocEnabled = dynamicAllocProp != null && dynamicAllocProp.equals(Boolean.TRUE.toString());
    if (dynamicAllocEnabled) {
      for (int i = 0; i < argArray.length; i++) {
        if (argArray[i] == null) continue;

        // If user specifies num of executors, or if user tries to disable dynamic allocation for his application
        // by setting some conf params to falase, we need to ignore these settings to enforce the application 
        // uses dynamic allocation.
        if (argArray[i].equals(SparkJobArg.NUM_EXECUTORS.sparkParamName) // --num-executors
          || (
            argArray[i].equals(SparkJobArg.SPARK_CONF_PREFIX.sparkParamName) // --conf
            && (argArray[i+1].startsWith(SPARK_CONF_SHUFFLE_SERVICE_ENABLED) // spark.shuffle.service.enabled=
              || argArray[i+1].startsWith(SPARK_CONF_DYNAMIC_ALLOC_ENABLED)) // spark.dynamicAllocation.enabled
        )) {

          logger.info("Spark cluster enforces dynamic resource allocation. Ignore user param: " 
            + argArray[i] + " " + argArray[i+1]);
          argArray[i] = null;
          argArray[++i] = null;
        }
      }
    }
  }

  private static void handleNodeLabeling(String[] argArray) {
    // Check if yarn cluster node labeling is enabled
    // We detect the cluster settings to automatically enforce node labeling by ignoring
    // user queue params.
    // This is an automatic policy, rather than a configurable policy like dynamic allocation.
    Configuration conf = new Configuration();
    boolean nodeLabelingEnabled = conf.getBoolean(YARN_CONF_NODE_LABELING_ENABLED, false);

    if (nodeLabelingEnabled) {
      for (int i = 0; i < argArray.length; i++) {
        if (argArray[i] == null) continue;
        // If yarn cluster enables node labeling, applications should be submitted to a default
        // queue by a default conf(spark.yarn.queue) in spark-defaults.conf 
        // We should ignore user-specified queue param to enforece the node labeling
        // (--queue test or --conf spark.yarn.queue=test)
        if ((argArray[i].equals(SparkJobArg.SPARK_CONF_PREFIX.sparkParamName) &&
             argArray[i+1].startsWith(SPARK_CONF_QUEUE))
            || (argArray[i].equals(SparkJobArg.QUEUE.sparkParamName))) {

          logger.info("Yarn cluster enforces node labeling. Ignore user param: "
            + argArray[i] + " " + argArray[i+1]);
          argArray[i] = null;
          argArray[++i] = null;
        }
      }
    }
  }

  private static String[] removeNullsFromArgArray(String[] argArray) {
    List<String> argList = new ArrayList(Arrays.asList(argArray));
    argList.removeAll(Collections.singleton(null));
    return argList.toArray(new String[0]);
  }
}
