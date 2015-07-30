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

package azkaban.jobtype;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;
import org.apache.pig.PigRunner;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;

import azkaban.jobExecutor.ProcessJob;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;

public class HadoopSecurePigWrapper {

  private static final String PIG_DUMP_HADOOP_COUNTER_PROPERTY = "pig.dump.hadoopCounter";

  private static File pigLogFile;

  private static boolean securityEnabled;

  private static Props props;

  private static final Logger logger;

  static {
    logger = Logger.getRootLogger();
  }

  private static UserGroupInformation getSecureProxyUser(String userToProxy)
      throws Exception {
    String filelocation =
        System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    if (filelocation == null) {
      throw new RuntimeException("hadoop token information not set.");
    }
    if (!new File(filelocation).exists()) {
      throw new RuntimeException("hadoop token file doesn't exist.");
    }

    logger.info("Found token file " + filelocation);
    logger.info("Setting "
        + HadoopSecurityManager.MAPREDUCE_JOB_CREDENTIALS_BINARY + " to "
        + filelocation);

    System.setProperty(HadoopSecurityManager.MAPREDUCE_JOB_CREDENTIALS_BINARY,
        filelocation);
    UserGroupInformation loginUser = null;
    loginUser = UserGroupInformation.getLoginUser();

    logger.info("Current logged in user is " + loginUser.getUserName());
    logger.info("Creating proxy user.");

    UserGroupInformation proxyUser =
        UserGroupInformation.createProxyUser(userToProxy, loginUser);
    for (Token<?> token : loginUser.getTokens()) {
      proxyUser.addToken(token);
    }
    return proxyUser;
  }

  public static void main(final String[] args) throws Exception {
    String propsFile = System.getenv(ProcessJob.JOB_PROP_ENV);
    props = new Props(null, new File(propsFile));

    HadoopConfigurationInjector.injectResources(props);

    final Configuration conf = new Configuration();

    UserGroupInformation.setConfiguration(conf);
    securityEnabled = UserGroupInformation.isSecurityEnabled();
    pigLogFile = new File(System.getenv("PIG_LOG_FILE"));
    if (!shouldProxy(props)) {
      logger.info("Not proxying.");
      runPigJob(args);
      return;
    }

    UserGroupInformation proxyUser = null;
    String userToProxy = props.getString("user.to.proxy");
    if (securityEnabled) {
      proxyUser = getSecureProxyUser(userToProxy);
    } else {
      proxyUser = UserGroupInformation.createRemoteUser(userToProxy);
    }

    logger.info("Proxied as user " + userToProxy);
    proxyUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        runPigJob(args);
        return null;
      }
    });
  }

  @SuppressWarnings("deprecation")
  public static void runPigJob(String[] args) throws Exception {
    PigStats stats = null;
    if (props.getBoolean("pig.listener.visualizer", false) == true) {
      stats = PigRunner.run(args, new AzkabanPigListener(props));
    } else {
      stats = PigRunner.run(args, null);
    }

    dumpHadoopCounters(stats);

    if (stats.isSuccessful()) {
      return;
    }

    if (pigLogFile != null) {
      handleError(pigLogFile);
    }

    // see jira ticket PIG-3313. Will remove these when we use pig binary with
    // that patch.
    // /////////////////////
    System.out.println("Trying to do self kill, in case pig could not.");
    Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
    Thread[] threadArray = threadSet.toArray(new Thread[threadSet.size()]);
    for (Thread t : threadArray) {
      if (!t.isDaemon() && !t.equals(Thread.currentThread())) {
        System.out.println("Killing thread " + t);
        t.stop();
      }
    }
    System.exit(1);
    // ////////////////////
    throw new RuntimeException("Pig job failed.");
  }

  /**
   * Dump Hadoop counters for each of the M/R jobs in the given PigStats.
   * 
   * @param pigStats
   */
  private static void dumpHadoopCounters(PigStats pigStats) {
    try {
      if (props.getBoolean(PIG_DUMP_HADOOP_COUNTER_PROPERTY, false)) {
        if (pigStats != null) {
          JobGraph jGraph = pigStats.getJobGraph();
          Iterator<JobStats> iter = jGraph.iterator();
          while (iter.hasNext()) {
            JobStats jobStats = iter.next();
            System.out.println("\n === Counters for job: "
                + jobStats.getJobId() + " ===");
            Counters counters = jobStats.getHadoopCounters();
            if (counters != null) {
              for (Counters.Group group : counters) {
                System.out.println(" Counter Group: " + group.getDisplayName()
                    + " (" + group.getName() + ")");
                System.out.println("  number of counters in this group: "
                    + group.size());
                for (Counters.Counter counter : group) {
                  System.out.println("  - " + counter.getDisplayName() + ": "
                      + counter.getCounter());
                }
              }
            } else {
              System.out.println("There are no counters");
            }
          }
        } else {
          System.out.println("pigStats is null, can't dump Hadoop counters");
        }
      }
    } catch (Exception e) {
      System.out.println("Unexpected error: " + e.getMessage());
      e.printStackTrace(System.out);
    }
  }

  private static void handleError(File pigLog) throws Exception {
    System.out.println();
    System.out.println("Pig logfile dump:");
    System.out.println();
    try {
      BufferedReader reader = new BufferedReader(new FileReader(pigLog));
      String line = reader.readLine();
      while (line != null) {
        System.err.println(line);
        line = reader.readLine();
      }
      reader.close();
    } catch (FileNotFoundException e) {
      System.err.println("pig log file: " + pigLog + "  not found.");
    }
  }

  public static boolean shouldProxy(Props props) {
    String shouldProxy = props.getString(HadoopSecurityManager.ENABLE_PROXYING);
    return shouldProxy != null && shouldProxy.equals("true");
  }
}
