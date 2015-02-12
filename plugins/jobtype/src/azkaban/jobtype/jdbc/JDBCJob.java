package azkaban.jobtype.jdbc;


import azkaban.jobtype.hiveutils.util.AzkabanJobPropertyDescription;
import org.apache.log4j.Logger;

import java.util.Properties;

public class JDBCJob {
    private final static Logger LOG = Logger.getLogger("com.linkedin.jdbc.azkaban.jobtype.jdbc.JDBCJob");
    final private static String AZK_HIVE_ACTION = "azk.jdbc.action";

    @AzkabanJobPropertyDescription("jdbc driver name")
    public static final String JDBC_DRIVER = "jdbc.driver";
    @AzkabanJobPropertyDescription("Verbatim query to execute. Can also specify hive.query.nn where nn is a series of padded numbers, which will be executed in order")
    public static final String JDBC_UPDATE = "update";
    @AzkabanJobPropertyDescription("File to load query from.  Should be in same zip.")
    public static final String JDBC_QUERY_FILE = "jdbc.query.file";
    @AzkabanJobPropertyDescription("URL to retrieve the query from.")
    public static final String JDBC_QUERY_URL = "jdbc.query.url";

    private Properties p;

    public JDBCJob(String jobName, Properties p) {
        LOG.info("Initing jdbcJob: " + jobName + ", p:" + p);
        this.p = p;
    }

    public void run() throws AzkabanJDBCException {
        if (p == null) {
            throw new AzkabanJDBCException("Properties is null.  Can't continue");
        }
        if (!p.containsKey(JDBC_DRIVER)) {
            throw new AzkabanJDBCException("Need a driver mentioned against: " + JDBC_DRIVER);
        }

        String jdbcAction = JDBC_UPDATE;

        if (p.containsKey(AZK_HIVE_ACTION)) {
            jdbcAction = p.getProperty(AZK_HIVE_ACTION);
        }

        String driverName = p.getProperty(JDBC_DRIVER);
        // #azk.jdbc.action=update

        LOG.info("Starting: " + jdbcAction);
        JDBCBatchExecutor batchExecutor = new JDBCBatchExecutor(p , driverName);
        if (jdbcAction.equals(JDBC_UPDATE)) {
            batchExecutor.execute();
        }
    }

}