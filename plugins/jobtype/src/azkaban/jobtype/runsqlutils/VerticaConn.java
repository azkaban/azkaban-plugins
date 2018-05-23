package azkaban.jobtype.runsqlutils;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by Vace Kupecioglu on 24.10.2016.
 */
public class VerticaConn extends DBConnImpl {

    public VerticaConn(final String user, final String pass, final String host, final String db) {
        super(user, pass, host, db, "");
    }

    public VerticaConn(final String user, final String pass, final String host, final String db, final String backupServerNode) {
        super(user, pass, host, db, backupServerNode);
    }

    @Override
    public void open() throws SQLException {
        if (this.conn != null) {
            this.close();
        }

        final Properties connProp = new Properties();
        connProp.put("user", getUser());
        connProp.put("password", getPass());

        final String backupServerNode = getBackupServerNode();
        if (!backupServerNode.equals("")) {
            connProp.put("BackupServerNode", getBackupServerNode());
        }

        connProp.put("connectionLoadBalance", "true");
        conn = DriverManager.getConnection("jdbc:vertica://" + getHost() + ":5433/" + getDb(), connProp);
        conn.setAutoCommit(false);
    }
}
