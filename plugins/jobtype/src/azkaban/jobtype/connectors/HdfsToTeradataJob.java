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

package azkaban.jobtype.connectors;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableList;

import azkaban.jobtype.HadoopJavaJob;
import azkaban.utils.Props;

/**
 * Entry point for HDFSToTeradata Job that prepares/forks new JVM for the job.
 */
public class HdfsToTeradataJob extends HadoopJavaJob {
  private static final Logger logger = Logger.getLogger(HdfsToTeradataJob.class);

  public HdfsToTeradataJob(String jobid, Props sysProps, Props jobProps, Logger log) throws RuntimeException {
    super(jobid, sysProps, jobProps, log);
    jobProps.put(TdchConstants.LIB_JARS_KEY, sysProps.get(TdchConstants.LIB_JARS_KEY));

    //Initialize TDWallet if it hasn't on current JVM.
    TeraDataWalletInitializer.initialize(new File(getCwd()), new File(sysProps.get(TdchConstants.TD_WALLET_JAR)));
  }

  @Override
  protected String getJavaClass() {
    return HdfsToTeradataJobRunnerMain.class.getName();
  }

  /**
   * In addition to superclass's classpath, it adds jars from TDWallet unjarred folder.
   * {@inheritDoc}
   * @see azkaban.jobtype.HadoopJavaJob#getClassPaths()
   */
  @Override
  protected List<String> getClassPaths() {
    return ImmutableList.<String>builder()
                        //TDCH w. Tdwallet requires a classpath point to unjarred folder.
                        .add(TeraDataWalletInitializer.getTdchUnjarFolder())
                        .add(TeraDataWalletInitializer.getTdchUnjarFolder() + File.separator + "lib" + File.separator + "*")
                        .addAll(super.getClassPaths()).build();
  }
}
