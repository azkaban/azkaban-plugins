package azkaban.crypto;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import com.google.common.base.Preconditions;

public class Decryptions {
  private static final FsPermission USER_READ_PERMISSION_ONLY = new FsPermission(FsAction.READ,
                                                                                 FsAction.NONE,
                                                                                 FsAction.NONE);

  public static String decrypt(String cipheredText, String passphrasePath, FileSystem fs) throws IOException {
    Preconditions.checkNotNull(cipheredText);
    Preconditions.checkNotNull(passphrasePath);

    Path path = new Path(passphrasePath);
    Preconditions.checkArgument(fs.exists(path), "File does not exist at " + passphrasePath);
    Preconditions.checkArgument(fs.isFile(path), "Passphrase path is not a file. " + passphrasePath);

    FileStatus fileStatus = fs.getFileStatus(path);
    Preconditions.checkArgument(USER_READ_PERMISSION_ONLY.equals(fileStatus.getPermission()),
                                "Passphrase file should only have read only permission on only user. " + passphrasePath);

    Crypto crypto = new Crypto();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
      String passphrase = br.readLine();
      String decrypted = crypto.decrypt(cipheredText, passphrase);
      Preconditions.checkNotNull(decrypted, "Was not able to decrypt");
      return decrypted;
    }
  }
}