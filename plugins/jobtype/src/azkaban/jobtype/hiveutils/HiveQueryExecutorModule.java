package azkaban.jobtype.hiveutils;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEHISTORYFILELOC;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.SCRATCHDIR;

/**
 * Guice-like module for creating a Hive instance.  Easily turned back into
 * a full Guice module when we have need of it.
 */
class HiveQueryExecutorModule {
  private HiveConf hiveConf = null;
  private CliSessionState ss = null;

  HiveConf provideHiveConf() {
    if(this.hiveConf != null) return this.hiveConf;
    else this.hiveConf = new HiveConf();

    troublesomeConfig(HIVEHISTORYFILELOC, hiveConf);
    troublesomeConfig(SCRATCHDIR, hiveConf);

    return hiveConf;
  }

  private void troublesomeConfig(HiveConf.ConfVars value, HiveConf hc) {
    System.out.println("Troublesome config " + value + " = " + HiveConf.getVar(hc, value));
  }

  CliSessionState provideCliSessionState() {
    if(ss != null) return ss;
    ss = new CliSessionState(provideHiveConf());
    SessionState.start(ss);
    return ss;
  }

  //@Override
  protected void configure() { /** Nothing to do **/ }
}

