package azkaban.jobtype;

import azkaban.utils.Props;
import com.google.common.io.Files;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestBeelineHiveJob {
    Logger testLogger;
    private static final String WORKING_DIR = Files.createTempDir().getAbsolutePath() + "/TestBeelineHiveJob";

    @Before
    public void setUp() throws Exception {
        this.testLogger = Logger.getLogger("testLogger");
        final File workingDirFile = new File(WORKING_DIR);
        workingDirFile.mkdirs();
    }

    @Test
    public void testMainArguments() throws Exception {

        final Props sysProps = new Props();
        sysProps.put("database.type", "h2");
        sysProps.put("h2.path", "./h2");
        
        final String jobId = "test_job";
        final Props jobProps = new Props();
        jobProps.put("type", "beelinehive");
        jobProps.put("hive.script", "hivescript.sql");
        jobProps.put("hive.url", "localhost");

        final BeelineHiveJob job = new BeelineHiveJob(jobId, sysProps, jobProps, this.testLogger);
        final String jobArguments = job.getMainArguments();
        Assert.assertTrue(jobArguments.endsWith("-d org.apache.hive.jdbc.HiveDriver -f hivescript.sql "));
        Assert.assertTrue(jobArguments.startsWith("-u localhost"));
        Assert.assertFalse(jobArguments.contains(" -a "));
        System.out.println(job.getMainArguments());

    }
}
