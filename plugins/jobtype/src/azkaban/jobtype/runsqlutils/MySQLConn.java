package azkaban.jobtype.runsqlutils;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by Vace Kupecioglu on 12.01.2017.
 */
public class MySQLConn extends DBConnImpl {

    public MySQLConn(final String user, final String pass, final String host, final String db) {
        super(user, pass, host, db);
    }

    @Override
    public void open() throws SQLException {
        if (this.conn != null) {
            this.close();
        }

        final Properties connProp = new Properties();
        connProp.put("user", getUser());
        connProp.put("password", getPass());
        conn = DriverManager.getConnection("jdbc:mysql://" + getHost() + ":3306/" + getDb(), connProp);
        conn.setAutoCommit(false);
    }
}
