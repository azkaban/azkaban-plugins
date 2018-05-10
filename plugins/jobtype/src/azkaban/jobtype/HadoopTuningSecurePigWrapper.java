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

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.pig.PigRunner;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;

import azkaban.jobtype.tuning.TuningCommonConstants;
import azkaban.jobtype.tuning.TuningParameterUtils;
import azkaban.jobtype.tuning.TuningErrorHandler;
import azkaban.utils.Props;

/**
 * This class represent wrapper for running pig job with tuning enabled.
 */
public class HadoopTuningSecurePigWrapper {

  private static final String PIG_DUMP_HADOOP_COUNTER_PROPERTY = "pig.dump.hadoopCounter";
  public static final String WORKING_DIR = "working.dir";

  private static File pigLogFile;

  private static Props props;

  private static final Logger logger;

  /**
   * In case if job is failed because of auto tuning parameters, it will be retried with the best parameters
   * we have seen so far. Maximum number of try is 2.
   */
  private static int maxRetry = 2;
  private static int tryCount = 1;
  /**
   * Is job failed because of tuning parameters
   */
  private static boolean isTuningError = false;
  static {
    logger = Logger.getRootLogger();
  }

  /**
   * This function runs the pig job with tuning enabled. In case tuning job fails in first try, it asks for
   * last best parameter from tuning and re-run the job. In case tuning end point is not responding this job
   * will run with default parameters.
   * @param args
   * @throws Throwable
   */
  public static void main(final String[] args) throws Exception {
    Properties jobProps = HadoopSecureWrapperUtils.loadAzkabanProps();
    Props initialJobprops = new Props(null, jobProps);
    boolean retry = false;
    boolean firstTry = true;

    while (tryCount <= maxRetry && (retry || firstTry)) {
      tryCount++;
      firstTry = false;
      props = Props.clone(initialJobprops);
      props.put(TuningCommonConstants.AUTO_TUNING_RETRY, retry + "");

      TuningParameterUtils.updateAutoTuningParameters(props);

      HadoopTuningConfigurationInjector.prepareResourcesToInject(props,
          HadoopTuningSecurePigWrapper.getWorkingDirectory(props));

      HadoopTuningConfigurationInjector.injectResources(props, HadoopTuningSecurePigWrapper.getWorkingDirectory(props));

      // special feature of secure pig wrapper: we will append the pig error file
      // onto system out
      pigLogFile = new File(System.getenv("PIG_LOG_FILE"));

      try {
        if (HadoopSecureWrapperUtils.shouldProxy(jobProps)) {
          String tokenFile = System.getenv(HADOOP_TOKEN_FILE_LOCATION);
          UserGroupInformation proxyUser = HadoopSecureWrapperUtils.setupProxyUser(jobProps, tokenFile, logger);
          proxyUser.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              runPigJob(args);
              return null;
            }
          });
        } else {
          runPigJob(args);
        }
      } catch (Exception t) {
        retry = false;
        System.out.println("Error " + isTuningError + " tryCount:" + tryCount + ", maxRetry:" + maxRetry);
        if (isTuningError && tryCount<=maxRetry) {
          System.out.println("Error due to auto tuning parameters ");
          retry = true;
        }else
        {
          throw t;
        }
      }
    }
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

    if (isTuningError && tryCount<=maxRetry) {
      throw new RuntimeException("Pig job failed.");
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
            System.out.println("\n === Counters for job: " + jobStats.getJobId() + " ===");
            Counters counters = jobStats.getHadoopCounters();
            if (counters != null) {
              for (Counters.Group group : counters) {
                System.out.println(" Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")");
                System.out.println("  number of counters in this group: " + group.size());
                for (Counters.Counter counter : group) {
                  System.out.println("  - " + counter.getDisplayName() + ": " + counter.getCounter());
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
      isTuningError = false;
      TuningErrorHandler tuningErrorHandle = new TuningErrorHandler();
      while (line != null) {
        System.err.println(line);
        line = reader.readLine();
        //Checks if log have any predefined error pattern which can be caused by auto tuning parameters
        if (tuningErrorHandle.containsAutoTuningError(line)) {
          isTuningError = true;
        }
      }
      reader.close();
    } catch (FileNotFoundException e) {
      System.err.println("pig log file: " + pigLog + "  not found.");
    }
  }

  public static String getWorkingDirectory(Props props) {
    final String workingDir = props.getString(WORKING_DIR);
    if (workingDir == null) {
      return "";
    }

    return workingDir;
  }
}
