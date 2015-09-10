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

import azkaban.flow.CommonJobProperties;
import azkaban.utils.Props;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * HadoopConfigurationInjector is responsible for inserting links back to the
 * Azkaban UI in configurations and for automatically injecting designated job
 * properties into the Hadoop configuration.
 * <p>
 * It is assumed that the necessary links have already been loaded into the
 * properties. After writing the necessary links as a xml file as required by
 * Hadoop's configuration, clients may add the links as a default resource
 * using injectResources() so that they are included in any Configuration
 * constructed.
 */
public class HadoopConfigurationInjector {
  private static Logger _logger = Logger.getLogger(HadoopConfigurationInjector.class);
  private static final String INJECT_FILE = "hadoop-inject.xml";

  // Prefix for properties to be automatically injected into the Hadoop conf.
  public static final String INJECT_PREFIX = "hadoop-inject.";

  /*
   * To be called by the forked process to load the generated links and Hadoop
   * configuration properties to automatically inject.
   */
  public static void injectResources() {
    Configuration.addDefaultResource(INJECT_FILE);
  }

  /**
   * Writes out the XML configuration file that will be injected by the client
   * as a configuration resource.
   * <p>
   * This file will include a series of links injected by Azkaban as well as
   * any job properties that begin with the designated injection prefix.
   *
   * @param props The Azkaban properties
   * @param workingDir The Azkaban job working directory
   */
  public static void prepareResourcesToInject(Props props, String workingDir) {
    try {
      Configuration conf = new Configuration(false);

      // First, inject a series of Azkaban links. These are equivalent to
      // CommonJobProperties.[EXECUTION,WORKFLOW,JOB,JOBEXEC,ATTEMPT]_LINK
      addHadoopProperties(props);

      // Next, automatically inject any properties that begin with the
      // designated injection prefix.
      Map<String, String> confProperties = props.getMapByPrefix(INJECT_PREFIX);

      for (Map.Entry<String, String> entry : confProperties.entrySet()) {
        String confKey = entry.getKey().replace(INJECT_PREFIX, "");
        String confVal = entry.getValue();
        if (confVal != null) {
          conf.set(confKey, confVal);
        }
      }

      // Now write out the configuration file to inject.
      File file = getConfFile(props, workingDir, INJECT_FILE);
      OutputStream xmlOut = new FileOutputStream(file);
      conf.writeXml(xmlOut);
      xmlOut.close();
    } catch (Throwable e) {
      _logger.error("Encountered error while preparing the Hadoop configuration resource file", e);
    }
  }

  private static void addHadoopProperty(Props props, String propertyName) {
      props.put(INJECT_PREFIX + propertyName, props.get(propertyName));
  }

  private static void addHadoopProperties(Props props) {
    String[] propsToInject = new String[]{
        CommonJobProperties.EXEC_ID,
        CommonJobProperties.JOB_ID,
        CommonJobProperties.PROJECT_NAME,
        CommonJobProperties.PROJECT_VERSION,
        CommonJobProperties.EXECUTION_LINK,
        CommonJobProperties.JOB_LINK,
        CommonJobProperties.WORKFLOW_LINK,
        CommonJobProperties.JOBEXEC_LINK,
        CommonJobProperties.ATTEMPT_LINK,
        CommonJobProperties.OUT_NODES,
        CommonJobProperties.IN_NODES,
        CommonJobProperties.PROJECT_LAST_CHANGED_DATE,
        CommonJobProperties.PROJECT_LAST_CHANGED_BY
    };

    for(String propertyName : propsToInject) {
      addHadoopProperty(props, propertyName);
    }
  }

  /**
   * Resolve the location of the file containing the configuration file.
   *
   * @param props The Azkaban properties
   * @param workingDir The Azkaban job working directory
   * @param fileName The desired configuration file name
   */
  public static File getConfFile(Props props, String workingDir, String fileName) {
    File jobDir = new File(workingDir, getDirName(props));
    if (!jobDir.exists()) {
      jobDir.mkdir();
    }
    return new File(jobDir, fileName);
  }

  /**
   * For classpath reasons, we'll put each link file in a separate directory.
   * This must be called only after the job id has been inserted by the job.
   *
   * @param props The Azkaban properties
   */
  public static String getDirName(Props props) {
    String dirSuffix = props.get("azkaban.flow.nested.path");

    if ((dirSuffix == null) || (dirSuffix.length() == 0)) {
      dirSuffix = props.get("azkaban.job.id");
      if ((dirSuffix == null) || (dirSuffix.length() == 0)) {
        throw new RuntimeException("azkaban.flow.nested.path and azkaban.job.id were not set");
      }
    }

    return "_resources_" + dirSuffix;
  }

  /**
   * Gets the path to the directory in which the generated links and Hadoop
   * conf properties files are written.
   *
   * @param props The Azkaban properties
   * @param workingDir The Azkaban job working directory
   */
  public static String getPath(Props props, String workingDir) {
    return new File(workingDir, getDirName(props)).toString();
  }

  /**
   * Loads an Azkaban property into the Hadoop configuration.
   *
   * @param props The Azkaban properties
   * @param conf The Hadoop configuration
   * @param name The property name to load from the Azkaban properties into the Hadoop configuration
   */
  public static void loadProp(Props props, Configuration conf, String name) {
    String prop = props.get(name);
    if (prop != null) {
      conf.set(name, prop);
    }
  }
}
