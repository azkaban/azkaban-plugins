/*
 * Copyright 2014-2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.jobtype.connectors.gobblin;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import azkaban.flow.CommonJobProperties;
import azkaban.jobtype.HadoopJavaJob;
import azkaban.jobtype.connectors.gobblin.helper.IPropertiesValidator;
import azkaban.jobtype.connectors.gobblin.helper.MySqlToHdfsValidator;
import azkaban.utils.Props;


/**
 * Integration Azkaban with Gobblin. It prepares job properties for Gobblin and utilizes HadoopJavaJob to kick off the job
 */
public class GobblinHadoopJob extends HadoopJavaJob {
  private static final String GOBBLIN_PRESET_COMMON_PROPERTIES_FILE_NAME = "common.properties";
  private static Map<GobblinPresets, Properties> gobblinPresets;

  public GobblinHadoopJob(String jobid, Props sysProps, Props jobProps, Logger log) {
    super(jobid, sysProps, jobProps, log);
    initializePresets();

    jobProps.put(HadoopJavaJob.JOB_CLASS, "gobblin.azkaban.AzkabanJobLauncher");
    jobProps.put("job.name", jobProps.get(CommonJobProperties.JOB_ID));
    jobProps.put("launcher.type", "MAPREDUCE"); //Azkaban only supports MR mode
    jobProps.put("job.jars", sysProps.get("jobtype.classpath")); //This will be utilized by Gobblin and put jars into distributed cache
    jobProps.put("fs.uri", sysProps.get("fs.uri")); //Azkaban should only support HDFS

    loadPreset();
    transformProperties();
  }

  /**
   * Initializes presets and cache it into preset map. As presets do not change while server is up,
   * this initialization happens only once per JVM.
   */
  private void initializePresets() {
    if (gobblinPresets == null) {
      synchronized (GobblinHadoopJob.class) {
        if (gobblinPresets == null) {
          gobblinPresets = Maps.newHashMap();
          String gobblinPresetDirName = sysProps.getString(GobblinConstants.GOBBLIN_PRESET_DIR_KEY);
          File gobblinPresetDir = new File(gobblinPresetDirName);
          File[] presetFiles = gobblinPresetDir.listFiles();
          if (presetFiles == null) {
            return;
          }

          File commonPropertiesFile = new File(gobblinPresetDir, GOBBLIN_PRESET_COMMON_PROPERTIES_FILE_NAME);
          if (!commonPropertiesFile.exists()) {
            throw new IllegalStateException("Gobbline preset common properties file is missing "
                + commonPropertiesFile.getAbsolutePath());
          }

          for (File f : presetFiles) {
            if (GOBBLIN_PRESET_COMMON_PROPERTIES_FILE_NAME.equals(f.getName())) { //Don't load common one itself.
              continue;
            }

            if (f.isFile()) {
              Properties prop = new Properties();
              try (InputStream commonIs = new BufferedInputStream(new FileInputStream(commonPropertiesFile));
                  InputStream presetIs = new BufferedInputStream(new FileInputStream(f))) {
                prop.load(commonIs);
                prop.load(presetIs);

                String presetName = f.getName().substring(0, f.getName().lastIndexOf('.')); //remove extension from the file name
                gobblinPresets.put(GobblinPresets.fromName(presetName), prop);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          }
        }
      }
    }
  }

  /**
   * If input parameter has preset value, it will load set of properties into job property for the Gobblin job.
   * Also, if user wants to validates the job properties(enabled by default), it will validate it based on the preset where
   * preset is basically used as a proxy to the use case.
   */
  private void loadPreset() {
    String presetName = jobProps.get(GobblinConstants.GOBBLIN_PRESET_KEY);
    if (presetName == null) {
      return;
    }

    GobblinPresets preset = GobblinPresets.fromName(presetName);
    Properties presetProperties = gobblinPresets.get(preset);
    if (presetProperties == null) {
      throw new IllegalArgumentException("Preset " + presetName + " is not supported. Supported presets: "
          + gobblinPresets.keySet());
    }

    getLog().info("Loading preset " + presetName + " : " + presetProperties);
    Map<String, String> skipped = Maps.newHashMap();
    for (String key : presetProperties.stringPropertyNames()) {
      if (jobProps.containsKey(key)) {
        skipped.put(key, jobProps.get(key));
        continue;
      }
      jobProps.put(key, presetProperties.getProperty(key));
    }
    getLog().info("Loaded preset " + presetName);
    if (!skipped.isEmpty()) {
      getLog().info("Skipped some properties from preset as already exists in job properties. Skipped: " + skipped);
    }

    if (jobProps.getBoolean(GobblinConstants.GOBBLIN_PROPERTIES_HELPER_ENABLED_KEY, true)) {
      getValidator(preset).validate(jobProps);
    }
  }

  /**
   * Transform property to make it work for Gobblin.
   *
   * e.g: Gobblin fails when there's semicolon in SQL query as it just appends " and 1=1;" into the query
   * and makes the syntax incorrect. As having semicolon is a correct syntax, Azkaban will remove it for user
   * to make it work with Gobblin.
   */
  private void transformProperties() {
    //Gobblin does not accept the SQL query ends with semi-colon
    String query = jobProps.getString("source.querybased.query", null);
    int idx = -1;
    if (query != null && (idx = query.indexOf(';')) >= 0) {
      query = query.substring(0, idx);
      jobProps.put("source.querybased.query", query);
    }
  }

  /**
   * Factory method that provides IPropertiesValidator based on preset in runtime.
   * Using factory method pattern as it is expected to grow.
   * @param preset
   * @return IPropertiesValidator
   */
  private static IPropertiesValidator getValidator(GobblinPresets preset) {
    Objects.requireNonNull(preset);
    switch (preset) {
      case MYSQL_TO_HDFS:
        return new MySqlToHdfsValidator();
      default:
        throw new UnsupportedOperationException("Preset " + preset + " is not supported");
    }
  }
}
