package azkaban.jobtype.connectors.teradata;

import static org.mockito.Mockito.*;

import static azkaban.jobtype.connectors.teradata.TdchConstants.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.junit.Ignore;

import junit.framework.Assert;
import azkaban.utils.Props;

@Ignore
public class HdfsToTeradataTest {

  @Test
  public void errorTableTest() throws FileNotFoundException, IOException {
    Properties baseProperties = new Properties();
    baseProperties.put(TD_HOSTNAME_KEY, "test");
    baseProperties.put(TD_USERID_KEY, "test");
    baseProperties.put(TD_ENCRYPTED_CREDENTIAL_KEY, "test");
    baseProperties.put(TD_CRYPTO_KEY_PATH_KEY, "test");
    baseProperties.put(AVRO_SCHEMA_PATH_KEY, "test");
    baseProperties.put(SOURCE_HDFS_PATH_KEY, "test");

//    Properties properties = new Properties(baseProperties);
//
//    String targetTbl = "db.target_table";
//    properties.put(TARGET_TD_TABLE_NAME_KEY, targetTbl);
//    HdfsToTeradataJobRunnerMain job = new HdfsToTeradataJobRunnerMain(properties);
//    job = spy(job);
//
//    doReturn("test").when(job).decryptPassword(any(), any());
//
//    String actual = job.assignErrorTable(new Props(null, properties));
//    Assert.assertEquals("target_table", actual);
  }
}
