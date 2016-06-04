package azkaban.crypto;

import java.util.Base64;
import java.util.Map;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

public class Crypto implements ICrypto {
  private static final Logger logger = Logger.getLogger(Crypto.class);
  private static ObjectMapper MAPPER = new ObjectMapper();
  private final Map<Version, ICrypto> cryptos;

  public Crypto() {
    this.cryptos = ImmutableMap.<Version, ICrypto>builder()
                                  .put(Version.V1_0, new CryptoV1())
                                  .put(Version.V1_1, new CryptoV1_1())
                                  .build();
  }

  @Override
  public String encrypt(String plaintext, String passphrase, Version cryptoVersion) {
    ICrypto crypto = cryptos.get(cryptoVersion);
    return crypto.encrypt(plaintext, passphrase, cryptoVersion);
  }

  @Override
  public String decrypt(String cipheredText, String passphrase) {
    try {
      String jsonStr = ICrypto.decode(cipheredText);
      JsonNode json = MAPPER.readTree(jsonStr);
      String ver = json.get(ICrypto.VERSION_IDENTIFIER).asText();

      ICrypto crypto = cryptos.get(Version.fromVerString(ver));
      Preconditions.checkNotNull(crypto);
      return crypto.decrypt(cipheredText, passphrase);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static String encode(String s) {
    return Base64.getEncoder().encodeToString(s.getBytes());
  }

  public static String decode(String s) {
    return new String(Base64.getDecoder().decode(s));
  }
}