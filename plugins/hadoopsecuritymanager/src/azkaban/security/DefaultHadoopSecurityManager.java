/*
 * Copright 2011 LinkedIn Corp.
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

package azkaban.security;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;
import azkaban.utils.Props;

/**
 * This is just get by compile, or in the case of no hadoop installation.
 * Otherwise, there needs to be a real HadoopSecurityManger plugin for the
 * hadoop installation, even if the hadoop installation is not security enabled.
 */
public class DefaultHadoopSecurityManager extends HadoopSecurityManager {

  private static final Logger logger = Logger
      .getLogger(DefaultHadoopSecurityManager.class);

  private static HadoopSecurityManager hsmInstance = null;

  private DefaultHadoopSecurityManager(Props props) {
    logger.info("Default Hadoop Security Manager is used. Only do this on "
        + "a non-hadoop cluster!");
  }

  public static HadoopSecurityManager getInstance(Props props)
      throws HadoopSecurityManagerException, IOException {
    if (hsmInstance == null) {
      synchronized (DefaultHadoopSecurityManager.class) {
        if (hsmInstance == null) {
          logger.info("getting new instance");
          hsmInstance = new DefaultHadoopSecurityManager(props);
        }
      }
    }
    return hsmInstance;
  }

  @Override
  public UserGroupInformation getProxiedUser(String toProxy)
      throws HadoopSecurityManagerException {
    throw new HadoopSecurityManagerException(
        "No real Hadoop Security Manager is set!");
  }

  /**
   * Create a proxied user, taking all parameters, including which user to proxy
   * from provided Properties.
   */
  @Override
  public UserGroupInformation getProxiedUser(Props prop)
      throws HadoopSecurityManagerException {
    throw new HadoopSecurityManagerException(
        "No real Hadoop Security Manager is set!");
  }

  @Override
  public boolean isHadoopSecurityEnabled()
      throws HadoopSecurityManagerException {
    throw new HadoopSecurityManagerException(
        "No real Hadoop Security Manager is set!");
  }

  @Override
  public FileSystem getFSAsUser(String user)
      throws HadoopSecurityManagerException {
    throw new HadoopSecurityManagerException(
        "No real Hadoop Security Manager is set!");
  }

  @Override
  public void prefetchToken(File tokenFile, String userToProxy, Logger logger)
      throws HadoopSecurityManagerException {
    throw new HadoopSecurityManagerException(
        "No real Hadoop Security Manager is set!");
  }

  @Override
  public void cancelTokens(File tokenFile, String userToProxy, Logger logger)
      throws HadoopSecurityManagerException {
  }

  @Override
  public void prefetchToken(File tokenFile, Props props, Logger logger)
      throws HadoopSecurityManagerException {
    throw new HadoopSecurityManagerException(
        "No real Hadoop Security Manager is set!");
  }

}
