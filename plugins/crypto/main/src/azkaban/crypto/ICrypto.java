package azkaban.crypto;

import java.util.Base64;

/**
 * Encrypts plain text and decrypts ciphered text.
 */
public interface ICrypto {
  static final String VERSION_IDENTIFIER = "ver";

  /**
   * Encrypts plain text using pass phrase and crypto version.
   *
   * @param plaintext The plain text (secret) need to be encrypted.
   * @param passphrase Passphrase that will be used as a key to encrypt
   * @param cryptoVersion Version of this encryption.
   * @return A ciphered text, Base64 encoded.
   */
  public String encrypt (String plaintext, String passphrase, Version cryptoVersion);

  /**
   * Decrypts ciphered text.
   * @param cipheredText Base64 encoded ciphered text
   * @param passphrase Passphrase that was used as a key to encrypt the ciphered text
   * @return plain text String
   */
  public String decrypt (String cipheredText, String passphrase);

  public static String encode(String s) {
    return new String(Base64.getEncoder().encode(s.getBytes()));
  }

  public static String decode(String s) {
    return new String(Base64.getDecoder().decode(s));
  }
}