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

package azkaban.jobtype.connectors;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;
import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.jobtype.*;
import azkaban.jobtype.javautils.JobUtils;

import com.teradata.hadoop.tool.TeradataImportTool;

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

public class TeradataToHdfsJobRunnerMain {
  private static final int DEFAULT_NO_MAPPERS = 8;

  private final Properties _jobProps;
  private final TdchParameters _params;
  private final Logger _logger;

  public TeradataToHdfsJobRunnerMain() throws FileNotFoundException, IOException {
    _logger = JobUtils.initJobLogger();
    _jobProps = HadoopSecureWrapperUtils.loadAzkabanProps();

    Props props = new Props(null, _jobProps);
    HadoopConfigurationInjector.injectResources(props);
    UserGroupInformation.setConfiguration(new Configuration());
    props.getString(HadoopSecurityManager.USER_TO_PROXY); //Check required field.

    _params = TdchParameters.builder()
                            .mrParams(TdchConstants.MAP_REDUCE_PARAMS)
                            .libJars(props.getString(TdchConstants.LIB_JARS_KEY))
                            .tdJdbcClassName(TdchConstants.TERADATA_JDBCDRIVER_CLASSNAME)
                            .teradataHostname(props.getString(TdchConstants.TD_HOSTNAME_KEY))
                            .fileFormat(_jobProps.getProperty(TdchConstants.HDFS_FILE_FORMAT_KEY))
                            .fieldSeparator(_jobProps.getProperty(TdchConstants.HDFS_FIELD_SEPARATOR_KEY))
                            .jobType(TdchConstants.TDCH_JOB_TYPE)
                            .userName(props.getString(TdchConstants.TD_USERID_KEY))
                            .tdPassword(String.format(TdchConstants.TD_WALLET_FORMAT, props.getString(TdchConstants.TD_USERID_KEY)))
                            .avroSchemaPath(_jobProps.getProperty(TdchConstants.AVRO_SCHEMA_PATH_KEY))
                            .sourceTdTableName(_jobProps.getProperty(TdchConstants.SOURCE_TD_TABLE_NAME_KEY))
                            .sourceQuery(_jobProps.getProperty(TdchConstants.SOURCE_TD_QUERY_NAME_KEY))
                            .targetHdfsPath(props.getString(TdchConstants.TARGET_HDFS_PATH_KEY))
                            .tdRetrieveMethod(_jobProps.getProperty(TdchConstants.TD_RETRIEVE_METHOD_KEY))
                            .numMapper(DEFAULT_NO_MAPPERS)
                            .build();
  }

  public void run() throws IOException, InterruptedException {
    String jobName = System.getenv(AbstractProcessJob.JOB_NAME_ENV);
    _logger.info("Running job " + jobName);

    if (HadoopSecureWrapperUtils.shouldProxy(_jobProps)) {
      String tokenFile = System.getenv(HADOOP_TOKEN_FILE_LOCATION);
      UserGroupInformation proxyUser =
          HadoopSecureWrapperUtils.setupProxyUser(_jobProps, tokenFile, _logger);

      proxyUser.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          runCopyTdToHdfs();
          return null;
        }
      });
    } else {
      runCopyTdToHdfs();
    }
  }

  private void runCopyTdToHdfs() throws IOException {
    if (Boolean.valueOf(_jobProps.getProperty("force.output.overwrite", "false").trim())) {
      Path path = new Path(_jobProps.getProperty(TdchConstants.TARGET_HDFS_PATH_KEY));
      _logger.info("Deleting output directory " + path.toUri());
      JobConf conf = new JobConf();
      path.getFileSystem(conf).delete(path, true);
    }

    _logger.info("Executing " + TeradataToHdfsJobRunnerMain.class.getSimpleName() + " with " + _params);
    TeradataImportTool.main(_params.toTdchParams());
  }

  /**
   * Entry point of job process.
   *
   * @param args
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {
    new TeradataToHdfsJobRunnerMain().run();
  }
}
