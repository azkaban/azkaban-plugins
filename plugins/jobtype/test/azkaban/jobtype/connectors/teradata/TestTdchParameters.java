package azkaban.jobtype.connectors.teradata;

import static azkaban.jobtype.connectors.teradata.TdchConstants.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import azkaban.jobtype.connectors.teradata.TdchParameters.Builder;

public class TestTdchParameters {
  private static String PASSWORD = "password";
  private Builder builder;

  @Before
  public void setup() {
    builder = TdchParameters.builder()
                            .mrParams("mrParams")
                            .tdJdbcClassName(TdchConstants.TERADATA_JDBCDRIVER_CLASSNAME)
                            .teradataHostname("teradataHostname")
                            .fileFormat(AVRO_FILE_FORMAT)
                            .jobType(TdchConstants.TDCH_JOB_TYPE)
                            .userName("userName")
                            .avroSchemaPath("avroSchemaPath")
                            .sourceHdfsPath("sourceHdfsPath")
                            .numMapper(TdchConstants.DEFAULT_NO_MAPPERS);
  }

  @Test
  public void testToTdchParam() {
    String targetTableName = "db.target";
    TdchParameters params = builder.targetTdTableName(targetTableName)
                                   .password(PASSWORD)
                                   .build();

    List<String> expected = getExpectedTdchParams();

    Assert.assertEquals(expected, Arrays.asList(params.toTdchParams()));
  }

  private ImmutableList<String> getExpectedTdchParams() {
    ImmutableList<String> expected = ImmutableList.<String>builder()
        .add("mrParams")
        .add("-url")
        .add("jdbc:teradata://teradataHostname/CHARSET=UTF8")
        .add("-classname")
        .add("com.teradata.jdbc.TeraDriver")
        .add("-fileformat")
        .add("avrofile")
        .add("-jobtype")
        .add("hdfs")
        .add("-username")
        .add("userName")
        .add("-nummappers")
        .add(Integer.toString(DEFAULT_NO_MAPPERS))
        .add("-password")
        .add("password")
        .add("-avroschemafile")
        .add("avroSchemaPath")
        .add("-sourcepaths")
        .add("sourceHdfsPath")
        .add("-targettable")
        .add("db.target")
        .add("-errortablename")
        .add("target")
        .build();

    return expected;
  }

  @Test
  public void testOtherParams() {
    String targetTableName = "db.target";
    String otherParams = "testKey1=testVal1,nummappers=24"; //nummappers is already assigned, thus it should be ignored.

    TdchParameters params = builder.targetTdTableName(targetTableName)
                                   .password(PASSWORD)
                                   .otherProperties(otherParams)
                                   .build();

    ImmutableList<String> expected = getExpectedTdchParams();
    expected = ImmutableList.<String>builder().addAll(expected)
                                              .add("-testKey1")
                                              .add("testVal1")
                                              .build();

    Assert.assertEquals(expected, Arrays.asList(params.toTdchParams()));
  }

  @Test
  public void testOtherParamsWithDuplicateKey() {
    String targetTableName = "db.target";
    String otherParams = "testKey1=testVal1,testKey2=testVal2";

    TdchParameters params = builder.targetTdTableName(targetTableName)
                                   .password(PASSWORD)
                                   .otherProperties(otherParams)
                                   .build();

    ImmutableList<String> expected = getExpectedTdchParams();
    expected = ImmutableList.<String>builder().addAll(expected)
                                              .add("-testKey1")
                                              .add("testVal1")
                                              .add("-testKey2")
                                              .add("testVal2")
                                              .build();

    Assert.assertEquals(expected, Arrays.asList(params.toTdchParams()));
  }

  @Test
  public void testErrorTblDerivedFromTargetTbl() {
    String targetTableName = "db.target";
    String expected = "target";

    TdchParameters params = builder.targetTdTableName(targetTableName)
                                   .password(PASSWORD)
                                   .build();

    Assert.assertEquals(expected, params.getTdErrorTableName().get());
  }

  @Test
  public void testErrorTblFromInput() {
    String targetTableName = "db.target";
    String expected = "errTbl";

    TdchParameters params = builder.targetTdTableName(targetTableName)
                                   .errorTdTableName(expected)
                                   .password(PASSWORD)
                                   .build();

    Assert.assertEquals(expected, params.getTdErrorTableName().get());
  }

  @Test
  public void testLongErrorTblInput() {
    String targetTblName = "db.target";
    String errTblName = "errTbl_errTbl_errTbl_errTbl_errTbl_errTbl_errTbl_errTbl_errTbl_errTbl";

    try {
      builder.targetTdTableName(targetTblName)
             .errorTdTableName(errTblName)
             .password(PASSWORD)
             .build();
      Assert.fail("Should have failed with long error table name");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalArgumentException);
      Assert.assertEquals("Error table name cannot exceed 24 chracters.", e.getMessage());
    }
  }

  @Test
  public void testFailWithoutCredential() {
    try {
      builder.build();
      Assert.fail("Should have failed with no credentials");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalArgumentException);
      Assert.assertEquals("Password is required.", e.getMessage());
    }
  }

  @Test
  public void testFailWithMultipleCredentials() {
    try {
      builder.password(PASSWORD)
             .credentialName("test")
             .build();
      Assert.fail("Should have failed with more than one credentials");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalArgumentException);
      Assert.assertEquals("Please use either credential name or password, not all of them.", e.getMessage());
    }
  }
}
