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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.log4j.Logger;

import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class HadoopSecurityManager_H_1_0 extends HadoopSecurityManager {

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

  private static final String HIVE_METASTORE_SASL_ENABLED =
      "hive.metastore.sasl.enabled";
  private static final String HIVE_METASTORE_KERBEROS_PRINCIPAL =
      "hive.metastore.kerberos.principal";
  private static final String HIVE_METASTORE_LOCAL = "hive.metastore.local";

  private static URLClassLoader ucl;

  private HadoopSecurityManager_H_1_0(Props props)
      throws HadoopSecurityManagerException, IOException {

    // for now, assume the same/compatible native library, the same/compatible
    // hadoop-core jar
    String hadoopHome = props.getString("hadoop.home", null);
    String hadoopConfDir = props.getString("hadoop.conf.dir", null);

    if (hadoopHome == null) {
      hadoopHome = System.getenv("HADOOP_HOME");
    }
    if (hadoopConfDir == null) {
      hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
    }

    List<URL> resources = new ArrayList<URL>();
    if (hadoopConfDir != null) {
      logger.info("Using hadoop config found in "
          + new File(hadoopConfDir).toURI().toURL());
      resources.add(new File(hadoopConfDir).toURI().toURL());
    } else if (hadoopHome != null) {
      logger.info("Using hadoop config found in "
          + new File(hadoopHome, "conf").toURI().toURL());
      resources.add(new File(hadoopHome, "conf").toURI().toURL());
    } else {
      logger.info("HADOOP_HOME not set, using default hadoop config.");
    }

    ucl = new URLClassLoader(resources.toArray(new URL[resources.size()]));

    conf = new Configuration();
    conf.setClassLoader(ucl);

    if (props.containsKey("fs.hdfs.impl.disable.cache")) {
      logger.info("Setting fs.hdfs.impl.disable.cache to "
          + props.get("fs.hdfs.impl.disable.cache"));
      conf.setBoolean("fs.hdfs.impl.disable.cache",
          Boolean.valueOf(props.get("fs.hdfs.impl.disable.cache")));
    }

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
          logger.info("Logging with " + keytabPrincipal + " and "
              + keytabLocation);
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

    logger.info("Hadoop Security Manager Initiated");
  }

  public static HadoopSecurityManager getInstance(Props props)
      throws HadoopSecurityManagerException, IOException {
    if (hsmInstance == null) {
      synchronized (HadoopSecurityManager_H_1_0.class) {
        if (hsmInstance == null) {
          logger.info("getting new instance");
          hsmInstance = new HadoopSecurityManager_H_1_0(props);
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
    // logger.info("Secure proxy configuration: Property " + s + " = " + value);
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
              logger.info("Created DFS token: " + fsToken.toString());
              logger.info("Token kind: " + fsToken.getKind());
              logger.info("Token id: " + fsToken.getIdentifier());
              logger.info("Token service: " + fsToken.getService());

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
              logger.info("Created JT token: " + mrdt.toString());
              logger.info("Token kind: " + mrdt.getKind());
              logger.info("Token id: " + mrdt.getIdentifier());
              logger.info("Token service: " + mrdt.getService());

              jc.getCredentials().addToken(mrdt.getService(), mrdt);
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
              logger.info("Tokens loaded in " + tokenFile.getAbsolutePath());
            }
          });
    } catch (Exception e) {
      e.printStackTrace();
      throw new HadoopSecurityManagerException("Failed to get hadoop tokens! "
          + e.getMessage() + e.getCause());

    }
  }

  private void cancelNameNodeToken(final Token<? extends TokenIdentifier> t,
      String userToProxy) throws HadoopSecurityManagerException {
    try {
      getProxiedUser(userToProxy).doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          cancelToken(t);
          return null;
        }

        private void cancelToken(Token<?> nt) throws IOException,
            InterruptedException {
          nt.cancel(conf);
        }
      });
    } catch (Exception e) {
      e.printStackTrace();
      throw new HadoopSecurityManagerException("Failed to cancel Token. "
          + e.getMessage() + e.getCause());
    }
  }

  private void cancelMRJobTrackerToken(
      final Token<? extends TokenIdentifier> t, String userToProxy)
      throws HadoopSecurityManagerException {
    try {
      getProxiedUser(userToProxy).doAs(new PrivilegedExceptionAction<Void>() {
        @SuppressWarnings("unchecked")
        @Override
        public Void run() throws Exception {
          cancelToken((Token<DelegationTokenIdentifier>) t);
          return null;
        }

        private void cancelToken(Token<DelegationTokenIdentifier> jt)
            throws IOException, InterruptedException {
          JobConf jc = new JobConf(conf);
          JobClient jobClient = new JobClient(jc);
          jobClient.cancelDelegationToken(jt);
        }
      });
    } catch (Exception e) {
      e.printStackTrace();
      throw new HadoopSecurityManagerException("Failed to cancel Token. "
          + e.getMessage() + e.getCause());
    }
  }

  private void cancelHiveToken(final Token<? extends TokenIdentifier> t,
      String userToProxy) throws HadoopSecurityManagerException {
    try {
      HiveConf hiveConf = new HiveConf();
      HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(hiveConf);
      hiveClient.cancelDelegationToken(t.encodeToUrlString());
    } catch (Exception e) {
      e.printStackTrace();
      throw new HadoopSecurityManagerException("Failed to cancel Token. "
          + e.getMessage() + e.getCause());
    }
  }

  @Override
  public void cancelTokens(File tokenFile, String userToProxy, Logger logger)
      throws HadoopSecurityManagerException {
    // nntoken
    Credentials cred = null;
    try {
      cred =
          Credentials.readTokenStorageFile(new Path(tokenFile.toURI()),
              new Configuration());
      for (Token<? extends TokenIdentifier> t : cred.getAllTokens()) {
        logger.info("Got token: " + t.toString());
        logger.info("Token kind: " + t.getKind());
        logger.info("Token id: " + new String(t.getIdentifier()));
        logger.info("Token service: " + t.getService());
        if (t.getKind().equals(new Text("HIVE_DELEGATION_TOKEN"))) {
          logger.info("Cancelling hive token " + new String(t.getIdentifier()));
          cancelHiveToken(t, userToProxy);
        } else if (t.getKind().equals(new Text("MAPREDUCE_DELEGATION_TOKEN"))) {
          logger.info("Cancelling mr job tracker token "
              + new String(t.getIdentifier()));
          cancelMRJobTrackerToken(t, userToProxy);
        } else if (t.getKind().equals(new Text("HDFS_DELEGATION_TOKEN"))) {
          logger.info("Cancelling namenode token "
              + new String(t.getIdentifier()));
          cancelNameNodeToken(t, userToProxy);
        } else {
          logger.info("unknown token type " + t.getKind());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  /*
   * Gets hadoop tokens for a user to run mapred/hive jobs on a secured cluster
   */
  @Override
  public synchronized void prefetchToken(final File tokenFile,
      final Props props, final Logger logger)
      throws HadoopSecurityManagerException {

    final String userToProxy = props.getString(USER_TO_PROXY);

    logger.info("Getting hadoop tokens for " + userToProxy);

    final Credentials cred = new Credentials();

    if (props.getBoolean(OBTAIN_HCAT_TOKEN, false)) {
      try {
        logger.info("Pre-fetching Hive MetaStore token from hive");

        HiveConf hiveConf = new HiveConf();
        logger.info("HiveConf.ConfVars.METASTOREURIS.varname "
            + hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname));
        logger.info("HIVE_METASTORE_SASL_ENABLED "
            + hiveConf.get(HIVE_METASTORE_SASL_ENABLED));
        logger.info("HIVE_METASTORE_KERBEROS_PRINCIPAL "
            + hiveConf.get(HIVE_METASTORE_KERBEROS_PRINCIPAL));
        logger.info("HIVE_METASTORE_LOCAL "
            + hiveConf.get(HIVE_METASTORE_LOCAL));

        HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(hiveConf);
        String hcatTokenStr =
            hiveClient.getDelegationToken(userToProxy, UserGroupInformation
                .getLoginUser().getShortUserName());
        Token<DelegationTokenIdentifier> hcatToken =
            new Token<DelegationTokenIdentifier>();
        hcatToken.decodeFromUrlString(hcatTokenStr);
        logger.info("Created hive metastore token: " + hcatTokenStr);
        logger.info("Token kind: " + hcatToken.getKind());
        logger.info("Token id: " + hcatToken.getIdentifier());
        logger.info("Token service: " + hcatToken.getService());
        cred.addToken(hcatToken.getService(), hcatToken);
      } catch (Exception e) {
        e.printStackTrace();
        logger.error("Failed to get hive metastore token." + e.getMessage()
            + e.getCause());
      } catch (Throwable t) {
        t.printStackTrace();
        logger.error("Failed to get hive metastore token." + t.getMessage()
            + t.getCause());
      }
    }

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
              logger.info("Here is the props for " + OBTAIN_NAMENODE_TOKEN
                  + ": " + props.getBoolean(OBTAIN_NAMENODE_TOKEN));
              if (props.getBoolean(OBTAIN_NAMENODE_TOKEN, false)) {
                FileSystem fs = FileSystem.get(conf);
                // check if we get the correct FS, and most importantly, the
                // conf
                logger.info("Getting DFS token from " + fs.getUri());
                Token<?> fsToken = fs.getDelegationToken(userToProxy);
                if (fsToken == null) {
                  logger.error("Failed to fetch DFS token for ");
                  throw new HadoopSecurityManagerException(
                      "Failed to fetch DFS token for " + userToProxy);
                }
                logger.info("Created DFS token: " + fsToken.toString());
                logger.info("Token kind: " + fsToken.getKind());
                logger.info("Token id: " + fsToken.getIdentifier());
                logger.info("Token service: " + fsToken.getService());
                cred.addToken(fsToken.getService(), fsToken);
              }

              if (props.getBoolean(OBTAIN_JOBTRACKER_TOKEN, false)) {
                JobClient jobClient = new JobClient(new JobConf());
                logger.info("Pre-fetching JT token from JobTracker");

                Token<DelegationTokenIdentifier> mrdt =
                    jobClient.getDelegationToken(new Text("mr token"));
                if (mrdt == null) {
                  logger.error("Failed to fetch JT token");
                  throw new HadoopSecurityManagerException(
                      "Failed to fetch JT token for " + userToProxy);
                }
                logger.info("Created JT token: " + mrdt.toString());
                logger.info("Token kind: " + mrdt.getKind());
                logger.info("Token id: " + mrdt.getIdentifier());
                logger.info("Token service: " + mrdt.getService());
                cred.addToken(mrdt.getService(), mrdt);
              }
            }
          });

      FileOutputStream fos = null;
      DataOutputStream dos = null;
      try {
        fos = new FileOutputStream(tokenFile);
        dos = new DataOutputStream(fos);
        cred.writeTokenStorageToStream(dos);
      } finally {
        if (dos != null) {
          dos.close();
        }
        if (fos != null) {
          fos.close();
        }
      }

      // stash them to cancel after use.
      logger.info("Tokens loaded in " + tokenFile.getAbsolutePath());

    } catch (Exception e) {
      e.printStackTrace();
      throw new HadoopSecurityManagerException("Failed to get hadoop tokens! "
          + e.getMessage() + e.getCause());
    } catch (Throwable t) {
      t.printStackTrace();
      throw new HadoopSecurityManagerException("Failed to get hadoop tokens! "
          + t.getMessage() + t.getCause());
    }
  }
}
