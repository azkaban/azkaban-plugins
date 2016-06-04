package azkaban.crypto;

import org.junit.Test;

import junit.framework.Assert;

public class EncryptionTest {

  @Test
  public void testEncryption() {
    String plainText = "test";
    String passphrase = "test1234";

    ICrypto crypto = new Crypto();

    for (Version ver : Version.values()) {
      String cipheredText = crypto.encrypt(plainText, passphrase, ver);
      Assert.assertEquals(plainText, crypto.decrypt(cipheredText, passphrase));
    }
  }

}