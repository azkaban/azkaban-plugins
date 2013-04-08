package azkaban.jobtype.hiveutils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Guice-like module for creating a Hive instance. Easily turned back into a
 * full Guice module when we have need of it.
 */
class HiveModule {
	/**
	 * Return a Driver that's connected to the real, honest-to-goodness Hive
	 * 
	 * @TODO: Better error checking
	 * @return Driver that's connected to Hive
	 */
	Driver provideHiveDriver() {
		HiveConf hiveConf = provideHiveConf();
		SessionState.start(hiveConf);

		return new Driver(hiveConf);
	}

	HiveConf provideHiveConf() {
		return new HiveConf(SessionState.class);
	}

	protected void configure() { /* Nothing to do */
	}
}
