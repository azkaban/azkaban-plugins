package azkaban.crypto;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Command line interface for user to encrypt plain text with passphrase.
 */
public class EncryptionCLI {
  private static final String PASSPHRASE_KEY = "k";
  private static final String PLAINTEXT_KEY = "p";
  private static final String VERSION_KEY = "v";
  private static final String HELP_KEY = "h";

  /**
   * Outputs ciphered text to STDOUT.
   *
   * usage: EncryptionCLI [-h] -k <pass phrase> -p <plainText> -v <crypto version>
   * -h,--help                       print this message
   * -k,--key <pass phrase>          Passphrase used for encrypting plain text
   * -p,--plaintext <plainText>      Plaintext that needs to be encrypted
   * -v,--version <crypto version>   Version it will use to encrypt Version: [1.0, 1.1]
   *
   * @param args
   * @throws ParseException
   */
  public static void main(String[] args) throws ParseException {
    CommandLineParser parser = new DefaultParser();

    if (parser.parse(createHelpOptions(), args, true).hasOption(HELP_KEY)) {
      new HelpFormatter().printHelp(EncryptionCLI.class.getSimpleName(), createOptions(), true);
      return;
    }

    CommandLine line = parser.parse(createOptions(), args);

    String passphraseKey = line.getOptionValue(PASSPHRASE_KEY);
    String plainText = line.getOptionValue(PLAINTEXT_KEY);
    String version = line.getOptionValue(VERSION_KEY);

    ICrypto crypto = new Crypto();
    String cipheredText = crypto.encrypt(plainText, passphraseKey, Version.fromVerString(version));
    System.out.println(cipheredText);
  }

  private static Options createHelpOptions() {
    Options options = new Options();
    options.addOption(Option.builder(HELP_KEY).longOpt("help")
                            .desc("print this message").build());
    return options;
  }

  private static Options createOptions() {
    Options options = createHelpOptions();

    options.addOption(Option.builder(PLAINTEXT_KEY).longOpt("plaintext").hasArg().required()
                            .desc("Plaintext that needs to be encrypted")
                            .argName("plainText").build());

    options.addOption(Option.builder(PASSPHRASE_KEY).longOpt("key").hasArg().required()
                            .desc("Passphrase used for encrypting plain text")
                            .argName("pass phrase").build());

    options.addOption(Option.builder(VERSION_KEY).longOpt("version").hasArg().required()
        .desc("Version it will use to encrypt Version: " + Version.versionStrings())
        .argName("crypto version").build());

    return options;
  }
}