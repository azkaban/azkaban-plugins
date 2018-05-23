package azkaban.jobtype.runsqlutils;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Vace Kupecioglu on 12.01.2017.
 */
public abstract class DBConnImpl implements DBConn {
    Connection conn;
    String host;
    String user;
    String pass;
    String db;
    String backupServerNode;

    @Override
    public String getHost() {
        return this.host;
    }

    public void setHost(final String host) {
        this.host = host;
    }

    @Override
    public String getUser() {
        return this.user;
    }

    public void setUser(final String user) {
        this.user = user;
    }

    @Override
    public String getPass() {
        return this.pass;
    }

    public void setPass(final String pass) {
        this.pass = pass;
    }

    @Override
    public String getDb() {
        return this.db;
    }

    public void setDb(final String db) {
        this.db = db;
    }

    @Override
    public String getBackupServerNode() {
        return this.backupServerNode;
    }

    public void setBackupServerNode(final String backupServerNode) {
        this.backupServerNode = backupServerNode;
    }

    public DBConnImpl(final String user, final String pass, final String host, final String db) {
        this.setUser(user.trim());
        this.setPass(pass.trim());
        this.setHost(host.trim());
        this.setDb(db.trim());
    }

    public DBConnImpl(final String user, final String pass, final String host, final String db, final String backupServerNode) {
        this.setUser(user.trim());
        this.setPass(pass.trim());
        this.setHost(host.trim());
        this.setDb(db.trim());
        this.setBackupServerNode(backupServerNode.trim());
    }

    @Override
    public void close() {
        if (this.conn != null) {
            DbUtils.closeQuietly(this.conn);
            this.conn = null;
        }
    }

    @Override
    public void runSql(final String sql) throws SQLException {
        boolean closeWhenReturned = false;
        if (this.conn == null) {
            this.open();
            closeWhenReturned = true;
        }
        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
            stmt.execute(sql);
            this.conn.commit();
        } finally {
            DbUtils.closeQuietly(stmt);
            if (closeWhenReturned) {
                close();
            }
        }
    }

    @Override
    public <T> List<T> getList(final String sql, final Object... params) throws SQLException {
        boolean closeWhenReturned = false;
        if (this.conn == null) {
            this.open();
            closeWhenReturned = true;
        }

        try {
            final QueryRunner qr = new QueryRunner();
            return qr.query(this.conn, sql, new ResultSetHandler<List<T>>() {
                @Override
                public List<T> handle(final ResultSet resultSet) throws SQLException {
                    final ArrayList<T> res = new ArrayList<T>();
                    while (resultSet.next()) {
                        final T t = (T) resultSet.getObject(1);
                        res.add(t);
                    }
                    return res;
                }
            }, params);

        } finally {
            if (closeWhenReturned) {
                close();
            }
        }
    }

    @Override
    public <T> T getFirst(final String sql, final Object... params) throws SQLException {
        final List<T> l = getList(sql, params);
        return l.size() > 0 ? l.get(0) : null;
    }
}
