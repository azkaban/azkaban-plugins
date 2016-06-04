package azkaban.crypto;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

public class DecryptionTest {

  @Test
  public void testV1_1() throws IOException {
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.DEBUG);

    String expected = "test";

    String ciphered = "eyJ2ZXIiOiIxLjEiLCJ2YWwiOiJpaE9CM2VzTzBad2F4cHZBV2Z5YUVicHZLQzJBWDJZZnVzS3hVWFN2R3A0PSJ9";
    String passphrase = "test1234";

    Crypto crypto = new Crypto();
    String actual = crypto.decrypt(ciphered, passphrase);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testV1() throws IOException {
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.DEBUG);

    String expected = "test";

    String ciphered = "eyJ2ZXIiOiIxLjAiLCJ2YWwiOiJOd1hRejdOMjBXUU05SXEzaE94RVZnPT0ifQ==";
    String passphrase = "test1234";

    Crypto crypto = new Crypto();
    String actual = crypto.decrypt(ciphered, passphrase);
    Assert.assertEquals(expected, actual);
  }
}