package azkaban.jobtype;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import azkaban.flow.CommonJobProperties;
import azkaban.jobtype.javautils.Whitelist;
import azkaban.utils.Props;

public class TestWhitelist {
  private static final String PROXY_USER_KEY = "user.to.proxy";
  private String[] whitelisted = {"whitelisted_1", "whitelisted_2"};
  private File temp;
  private Whitelist whitelist;

  @Before
  public void setup() throws IOException, URISyntaxException {
    temp = File.createTempFile(TestWhitelist.class.getSimpleName(), null);
    temp.deleteOnExit();

    try (BufferedWriter bw = new BufferedWriter(new FileWriter(temp))) {
      for (String s : whitelisted) {
        bw.write(s);
        bw.newLine();
      }
    }

    FileSystem fs = FileSystem.get(new URI("file:///"), new Configuration());
    whitelist = new Whitelist(temp.getAbsolutePath(), fs);
  }

  @After
  public void cleanup() {
    if (temp != null) {
      temp.delete();
    }
  }

  @Test
  public void testWhiteListed() throws IOException, URISyntaxException {
    for (String s : whitelisted) {
      whitelist.validateWhitelisted(s);

      Props props = new Props();
      props.put(PROXY_USER_KEY, s);
      whitelist.validateWhitelisted(props);

      props = new Props();
      props.put(CommonJobProperties.SUBMIT_USER, s);
      whitelist.validateWhitelisted(props);
    }
  }

  @Test
  public void testNotWhiteListed() throws IOException, URISyntaxException {

    String id = "not_white_listed";
    try {
      whitelist.validateWhitelisted(id);
      Assert.fail("Should throw UnsupportedOperationException");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof UnsupportedOperationException);
    }
  }

  @Test
  public void testProxyUserWhitelisted() throws IOException, URISyntaxException {
    String notAuthorized = "not_white_listed";

    for (String s : whitelisted) {
      Props props = new Props();
      props.put(PROXY_USER_KEY, s);
      props.put(CommonJobProperties.SUBMIT_USER, notAuthorized);
      whitelist.validateWhitelisted(props);
    }
  }

  @Test
  public void testProxyUserNotAuthorized() throws IOException, URISyntaxException {
    String notAuthorized = "not_white_listed";

    for (String authorized : whitelisted) {
      Props props = new Props();
      props.put(PROXY_USER_KEY, notAuthorized);
      props.put(CommonJobProperties.SUBMIT_USER, authorized);
      try {
        whitelist.validateWhitelisted(props);
        Assert.fail("Should throw UnsupportedOperationException");
      } catch (Exception e) {
        Assert.assertTrue(e instanceof UnsupportedOperationException);
      }
    }
  }
}