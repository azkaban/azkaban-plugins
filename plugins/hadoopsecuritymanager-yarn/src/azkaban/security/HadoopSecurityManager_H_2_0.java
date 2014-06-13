/*
 * Copyright 2011 LinkedIn Corp.
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;

import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.UndefinedPropertyException;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class HadoopSecurityManager_H_2_0 extends HadoopSecurityManager {

  private UserGroupInformation loginUser = null;
  private final static Logger logger = Logger
      .getLogger(HadoopSecurityManager.class);
  private Configuration conf;

  private String keytabLocation;
  private String keytabPrincipal;
  private boolean shouldProxy = false;
  private boolean securityEnabled = false;

  private static HadoopSecurityManager hsmInstance = null;
  private ConcurrentMap<String, UserGroupInformation> userUgiMap;
  private Map<String, Token<?>> nnTokens =
      new ConcurrentHashMap<String, Token<?>>();
  private Map<String, Token<DelegationTokenIdentifier>> jtTokens =
      new ConcurrentHashMap<String, Token<DelegationTokenIdentifier>>();

  private HadoopSecurityManager_H_2_0(Props props)
      throws HadoopSecurityManagerException, IOException {

    ClassLoader cl;

    String hadoopHome = System.getenv("HADOOP_HOME");
    String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");

    if (hadoopConfDir != null) {
      logger.info("Using hadoop config found in " + hadoopConfDir);
      cl =
          new URLClassLoader(new URL[] { new File(hadoopConfDir).toURI()
              .toURL() }, getClass().getClassLoader());
    } else if (hadoopHome != null) {
      logger.info("Using hadoop config found in " + hadoopHome);
      cl =
          new URLClassLoader(new URL[] { new File(hadoopHome, "conf").toURI()
              .toURL() }, getClass().getClassLoader());
    } else {
      logger.info("HADOOP_HOME not set, using default hadoop config.");
      cl = getClass().getClassLoader();
    }
    conf = new Configuration();
    conf.setClassLoader(cl);

    logger.info("Hadoop Security Manager Initiated");
    logger.info("hadoop.security.authentication set to "
        + conf.get("hadoop.security.authentication"));
    logger.info("hadoop.security.authorization set to "
        + conf.get("hadoop.security.authorization"));
    logger.info("DFS name " + conf.get("fs.default.name"));

    UserGroupInformation.setConfiguration(conf);

    securityEnabled = UserGroupInformation.isSecurityEnabled();
    if (securityEnabled) {
      logger.info("The Hadoop cluster has enabled security");
      shouldProxy = true;
      try {
        keytabLocation = props.getString(PROXY_KEYTAB_LOCATION);
        keytabPrincipal = props.getString(PROXY_USER);
      } catch (UndefinedPropertyException e) {
        throw new HadoopSecurityManagerException(e.getMessage());
      }

      // try login
      try {
        if (loginUser == null) {
          logger.info("No login user. Creating login user");
          UserGroupInformation.loginUserFromKeytab(keytabPrincipal,
              keytabLocation);
          loginUser = UserGroupInformation.getLoginUser();
          logger.info("Logged in with user " + loginUser);
        } else {
          logger.info("loginUser (" + loginUser
              + ") already created, refreshing tgt.");
          loginUser.checkTGTAndReloginFromKeytab();
        }
      } catch (IOException e) {
        throw new HadoopSecurityManagerException(
            "Failed to login with kerberos ", e);
      }

    }

    userUgiMap = new ConcurrentHashMap<String, UserGroupInformation>();
  }

  public static HadoopSecurityManager getInstance(Props props)
      throws HadoopSecurityManagerException, IOException {
    if (hsmInstance == null) {
      synchronized (HadoopSecurityManager_H_2_0.class) {
        if (hsmInstance == null) {
          logger.info("getting new instance");
          hsmInstance = new HadoopSecurityManager_H_2_0(props);
        }
      }
    }
    return hsmInstance;
  }

  /**
   * Create a proxied user based on the explicit user name, taking other
   * parameters necessary from properties file.
   *
   * @throws IOException
   */
  @Override
  public synchronized UserGroupInformation getProxiedUser(String userToProxy)
      throws HadoopSecurityManagerException {
    // don't do privileged actions in case the hadoop is not secured.
    if (userToProxy == null) {
      throw new HadoopSecurityManagerException("userToProxy can't be null");
    }

    UserGroupInformation ugi = userUgiMap.get(userToProxy);
    if (ugi == null) {
      logger.info("proxy user " + userToProxy
          + " not exist. Creating new proxy user");
      if (shouldProxy) {
        try {
          ugi =
              UserGroupInformation.createProxyUser(userToProxy,
                  UserGroupInformation.getLoginUser());
        } catch (IOException e) {
          e.printStackTrace();
          throw new HadoopSecurityManagerException(
              "Failed to create proxy user", e);
        }
      } else {
        ugi = UserGroupInformation.createRemoteUser(userToProxy);
      }
      userUgiMap.putIfAbsent(userToProxy, ugi);
    }

    return ugi;
  }

  /**
   * Create a proxied user, taking all parameters, including which user to proxy
   * from provided Properties.
   */
  @Override
  public UserGroupInformation getProxiedUser(Props userProp)
      throws HadoopSecurityManagerException {
    String userToProxy = verifySecureProperty(userProp, USER_TO_PROXY);
    UserGroupInformation user = getProxiedUser(userToProxy);
    if (user == null)
      throw new HadoopSecurityManagerException(
          "Proxy as any user in unsecured grid is not supported!");
    return user;
  }

  public String verifySecureProperty(Props props, String s)
      throws HadoopSecurityManagerException {
    String value = props.getString(s);
    if (value == null) {
      throw new HadoopSecurityManagerException(s + " not set in properties.");
    }
    return value;
  }

  @Override
  public FileSystem getFSAsUser(String user)
      throws HadoopSecurityManagerException {
    FileSystem fs;
    try {
      logger.info("Getting file system as " + user);
      UserGroupInformation ugi = getProxiedUser(user);

      if (ugi != null) {
        fs = ugi.doAs(new PrivilegedAction<FileSystem>() {

          @Override
          public FileSystem run() {
            try {
              return FileSystem.get(conf);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
      } else {
        fs = FileSystem.get(conf);
      }
    } catch (Exception e) {
      throw new HadoopSecurityManagerException("Failed to get FileSystem. ", e);
    }
    return fs;
  }

  public boolean shouldProxy() {
    return shouldProxy;
  }

  @Override
  public boolean isHadoopSecurityEnabled() {
    return securityEnabled;
  }

  /*
   * Gets hadoop tokens for a user to run mapred/pig jobs on a secured cluster
   */
  @Override
  public synchronized void prefetchToken(final File tokenFile,
      final String userToProxy, final Logger logger)
      throws HadoopSecurityManagerException {

    logger.info("Getting hadoop tokens for " + userToProxy);

    try {
      getProxiedUser(userToProxy).doAs(
          new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              getToken(userToProxy);
              return null;
            }

            private void getToken(String userToProxy)
                throws InterruptedException, IOException,
                HadoopSecurityManagerException {

              FileSystem fs = FileSystem.get(conf);
              // check if we get the correct FS, and most importantly, the conf
              logger.info("Getting DFS token from "
                  + fs.getCanonicalServiceName() + fs.getUri());
              Token<?> fsToken = fs.getDelegationToken(userToProxy);
              if (fsToken == null) {
                logger.error("Failed to fetch DFS token for ");
                throw new HadoopSecurityManagerException(
                    "Failed to fetch DFS token for " + userToProxy);
              }

              JobConf jc = new JobConf(conf);
              JobClient jobClient = new JobClient(jc);
              logger.info("Pre-fetching JT token: Got new JobClient: " + jc);

              Token<DelegationTokenIdentifier> mrdt =
                  jobClient.getDelegationToken(new Text("mr token"));
              if (mrdt == null) {
                logger.error("Failed to fetch JT token for ");
                throw new HadoopSecurityManagerException(
                    "Failed to fetch JT token for " + userToProxy);
              }

              jc.getCredentials().addToken(new Text("howdy"), mrdt);
              jc.getCredentials().addToken(fsToken.getService(), fsToken);

              FileOutputStream fos = null;
              DataOutputStream dos = null;
              try {
                fos = new FileOutputStream(tokenFile);
                dos = new DataOutputStream(fos);
                jc.getCredentials().writeTokenStorageToStream(dos);
              } finally {
                if (dos != null) {
                  dos.close();
                }
                if (fos != null) {
                  fos.close();
                }
              }
              // stash them to cancel after use.
              nnTokens.put(tokenFile.getName(), fsToken);
              jtTokens.put(tokenFile.getName(), mrdt);
              System.out.println("Total tokens: nn " + nnTokens.size() + " jt "
                  + jtTokens.size());
              logger.info("Tokens loaded in " + tokenFile.getAbsolutePath());
            }
          });
    } catch (Exception e) {
      e.printStackTrace();
      throw new HadoopSecurityManagerException("Failed to get hadoop tokens! "
          + e.getMessage() + e.getCause());

    }
  }

  @Override
  public synchronized void prefetchToken(final File tokenFile,
      final Props props, final Logger logger)
      throws HadoopSecurityManagerException {
    throw new HadoopSecurityManagerException(
        "prefetchToken(File, Props, Logger) not implemented");
  }

  @Override
  public void cancelTokens(File tokenFile, String userToProxy, Logger logger)
      throws HadoopSecurityManagerException {
    // nntoken
    String key = tokenFile.getName();

    if (jtTokens.containsKey(key)) {
      final Token<DelegationTokenIdentifier> jtToken = jtTokens.get(key);
      logger.info("Canceling jt token" + jtToken);
      jtTokens.remove(key);
      try {
        getProxiedUser(userToProxy).doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            cancelToken(jtToken);
            return null;
          }

          private void cancelToken(Token<DelegationTokenIdentifier> jt)
              throws IOException, InterruptedException {
            JobConf jc = new JobConf(conf);
            JobClient jobClient = new JobClient(jc);
            jobClient.cancelDelegationToken(jtToken);
          }
        });
      } catch (IOException e) {
        e.printStackTrace();
        throw new HadoopSecurityManagerException("Failed to cancel token", e);
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new HadoopSecurityManagerException("Failed to cancel token", e);
      }
    }

    if (nnTokens.containsKey(key)) {
      final Token<?> nnToken = nnTokens.get(key);
      logger.info("Canceling nn token" + nnToken);
      nnTokens.remove(key);
      try {
        getProxiedUser(userToProxy).doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            cancelToken(nnToken);
            return null;
          }

          private void cancelToken(Token<?> nt) throws IOException,
              InterruptedException {
            nt.cancel(conf);
          }
        });
      } catch (IOException e) {
        e.printStackTrace();
        throw new HadoopSecurityManagerException("Failed to cancel token", e);
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new HadoopSecurityManagerException("Failed to cancel token", e);
      }
    }
  }

}
