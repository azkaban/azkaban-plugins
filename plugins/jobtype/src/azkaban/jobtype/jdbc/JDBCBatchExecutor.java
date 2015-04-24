package azkaban.jobtype.jdbc;

import azkaban.jobtype.hiveutils.azkaban.HiveViaAzkabanException;
import azkaban.jobtype.hiveutils.azkaban.Utils;
import azkaban.jobtype.hiveutils.azkaban.Utils.QueryPropKeys;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Limitations: Currently cannot execute a query with line separators in it. (\n)
 *
 * @author rama
 *
 */
public class JDBCBatchExecutor {

    private final static Logger LOG = Logger.getLogger("com.linkedin.jdbc.azkaban.jobtype.jdbc.JDBCBatchExecutor");
    private final Connection connection;
    private String query;

    public JDBCBatchExecutor(Properties p, String driverName) throws AzkabanJDBCException {
        try {
            LOG.info("Initing for: " + p + ", driver: " + driverName);
            Class.forName(driverName);

            String url = p.getProperty("jdbcUrl");
            String name = p.getProperty("user");
            String pass = p.getProperty("password");
            this.connection = DriverManager.getConnection(url, name, pass);

            QueryPropKeys keys = new QueryPropKeys("query", "query", "filename", "scriptUrl");
            this.query = Utils.determineQuery(p, keys);
        } catch (SQLException e) {
            throw new AzkabanJDBCException("Error while connecting to server: " + p, e);
        } catch (HiveViaAzkabanException e) {
            throw new AzkabanJDBCException(e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            throw new AzkabanJDBCException("Driver not found: " + driverName);
        }
    }


    public void execute() throws AzkabanJDBCException {
        try {
            Statement statement = this.connection.createStatement();
            /*
	     * Introducing Transaction Behaviour
	     * */
	    connection.setAutoCommit(false);
	    for (String query: this.query.split("\\\n")) {
                LOG.info("Executing: " + query + ".");
                boolean result = statement.execute(query);
                LOG.info("Executed: " + query + ", executed: " + result);
            }
	    connection.commit();
        } catch (SQLException e) {
            throw new AzkabanJDBCException("Error while executing " + this.query, e);
        }
    }
    
    /*
    public static void main(String[] args) throws AzkabanJDBCException {
        Properties props = new Properties() {{
           put("query.01", "select * from abc");
           put("query.02", "create table abc1 select * from abc");
           put("query.03", "select * from abc1");
           put("jdbcUrl", "jdbc:mysql://localhost:3306/temp");
           put("user", "analytics");
           put("password", "password");
        }};
        
        new JDBCBatchExecutor(props, "com.mysql.jdbc.Driver").execute();
    }*/
}
