package azkaban.jobtype;

import azkaban.jobExecutor.AbstractJob;
import azkaban.utils.Props;
import azkaban.jobtype.runsqlutils.*;
import com.google.common.io.Files;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Vace Kupecioglu on 13.10.2016.
 */
public class RunSqlJob extends AbstractJob {
    public static final String WORKING_DIR = "working.dir";
    public static final String FILE = "file";

    Props jobProps;
    Props sysProps;

    public RunSqlJob(final String jobId, final Props sysProps, final Props jobProps, final Logger log) {
        super(jobId, log);
        this.jobProps = jobProps;
        this.sysProps = sysProps;
    }

    @Override
    public void run() throws Exception {
        if (!getJobProps().containsKey(FILE)) {
            throw new Exception(FILE + " parameter must be specified.");
        }

        final String db = getJobProps().getString("db", "vertica").trim();

        if (db.equals("")) {
            throw new Exception("You must specify a db name");
        }

        if (!areConnectionParamsSet(db)) {
            throw new Exception("Connection parameters are not set for db " + db);
        }

        final File sqlFile = new File(getWorkingDirectory(), getJobProps().getString(FILE));
        if (!sqlFile.exists()) {
            throw new Exception("Could not find the input file at: " + sqlFile.getAbsolutePath());
        }

        final String rawSql = Files.toString(sqlFile, Charset.forName("UTF-8"));
        final String sql = replaceParams(rawSql);
        info("Query to execute: ");
        info(sql);

        final DBConn conn = createConn(db, this.sysProps);
        conn.runSql(sql);
    }

    private String replaceParams(String sql) {
        final Pattern r = Pattern.compile("^param\\.(\\w*)$");
        for (final String key : this.jobProps.getKeySet()) {
            final Matcher m = r.matcher(key);
            if (m.find()) {
                final String paramName = m.group(1);
                final String paramValue = this.jobProps.getString(key);
                info("Replacing param " + paramName + " with value " + paramValue);
                sql = sql.replaceAll("\\{\\{" + paramName + "}}", paramValue);
            }
        }
        return sql;
    }

    private boolean areConnectionParamsSet(final String db) {
        return this.sysProps.containsKey("db." + db + ".user") &&
                this.sysProps.containsKey("db." + db + ".pass") &&
                this.sysProps.containsKey("db." + db + ".host") &&
                this.sysProps.containsKey("db." + db + ".db") &&
                this.sysProps.containsKey("db." + db + ".type");
    }

    private DBConn createConn(final String db, final Props sysProps) throws Exception {
        final String type = sysProps.getString("db." + db + ".type", "").trim();

        if (type.equalsIgnoreCase("mysql")) {
            return new MySQLConn(
                    sysProps.getString("db." + db + ".user"),
                    sysProps.getString("db." + db + ".pass"),
                    sysProps.getString("db." + db + ".host"),
                    sysProps.getString("db." + db + ".db")
            );
        }

        if (type.equalsIgnoreCase("vertica")) {
            return new VerticaConn(
                    sysProps.getString("db." + db + ".user"),
                    sysProps.getString("db." + db + ".pass"),
                    sysProps.getString("db." + db + ".host"),
                    sysProps.getString("db." + db + ".db"),
                    sysProps.getString("db." + db + ".backupServerNode", "")
            );
        }

        throw new Exception("Unsupported db type: " + type);
    }


    public Props getJobProps() {
        return this.jobProps;
    }

    public String getWorkingDirectory() {
        final String workingDir = getJobProps().getString(WORKING_DIR, "");
        if (workingDir == null) {
            return "";
        }

        return workingDir;
    }

}
