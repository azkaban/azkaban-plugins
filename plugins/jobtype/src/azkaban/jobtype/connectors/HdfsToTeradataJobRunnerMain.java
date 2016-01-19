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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.jobtype.*;
import azkaban.jobtype.javautils.JobUtils;
import azkaban.utils.Props;

import com.teradata.hadoop.tool.TeradataExportTool;

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

public class HdfsToTeradataJobRunnerMain {
  private static final int DEFAULT_NO_MAPPERS = 8;

  private final Properties _jobProps;
  private final TdchParameters _params;
  private final Logger _logger;

  public HdfsToTeradataJobRunnerMain() throws FileNotFoundException, IOException {
    _logger = JobUtils.initJobLogger();
    _jobProps = HadoopSecureWrapperUtils.loadAzkabanProps();

    Props props = new Props(null, _jobProps);
    HadoopConfigurationInjector.injectResources(props);
    UserGroupInformation.setConfiguration(new Configuration());

    _params = TdchParameters.builder()
                            .mrParams(_jobProps.getProperty(TdchConstants.HADOOP_CONFIG_KEY))
                            .libJars(props.getString(TdchConstants.LIB_JARS_KEY))
                            .tdJdbcClassName(TdchConstants.TERADATA_JDBCDRIVER_CLASSNAME)
                            .teradataHostname(props.getString(TdchConstants.TD_HOSTNAME_KEY))
                            .fileFormat(_jobProps.getProperty(TdchConstants.HDFS_FILE_FORMAT_KEY))
                            .fieldSeparator(_jobProps.getProperty(TdchConstants.HDFS_FIELD_SEPARATOR_KEY))
                            .jobType(TdchConstants.TDCH_JOB_TYPE)
                            .userName(props.getString(TdchConstants.TD_USERID_KEY))
                            .credentialName(String.format(TdchConstants.TD_WALLET_FORMAT, props.getString(TdchConstants.TD_CREDENTIAL_NAME)))
                            .avroSchemaPath(_jobProps.getProperty(TdchConstants.AVRO_SCHEMA_PATH_KEY))
                            .avroSchemaInline(_jobProps.getProperty(TdchConstants.AVRO_SCHEMA_INLINE_KEY))
                            .sourceHdfsPath(props.getString(TdchConstants.SOURCE_HDFS_PATH_KEY))
                            .targetTdTableName(props.getString(TdchConstants.TARGET_TD_TABLE_NAME_KEY))
                            .tdInsertMethod(_jobProps.getProperty(TdchConstants.TD_INSERT_METHOD_KEY))
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
          copyHdfsToTd();
          return null;
        }
      });
    } else {
      copyHdfsToTd();
    }
  }

  /**
   * Calling TDCH to move data from HDFS to Teradata
   *
   * @param args
   */
  private void copyHdfsToTd() {
    _logger.info(String.format("Executing %s with params: %s", HdfsToTeradataJobRunnerMain.class.getSimpleName(), _params));
    TeradataExportTool.main(_params.toTdchParams());
  }

  /**
   * Entry point of job process.
   *
   * @param args
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {
    new HdfsToTeradataJobRunnerMain().run();
  }
}
