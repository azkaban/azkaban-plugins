package azkaban.jobtype.javautils;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;


public class FileUtils {
  private static Logger logger = Logger.getLogger(FileUtils.class);

  /**
   * Delete file or directory.
   * (Apache FileUtils.deleteDirectory has a bug and is not working.)
   *
   * @param file
   * @throws IOException
   */
  public static void deleteFileOrDirectory(File file) throws IOException {
    if (!file.isDirectory()) {
      file.delete();
      return;
    }

    if (file.list().length == 0) { //Nothing under directory. Just delete it.
      file.delete();
      return;
    }

    for (String temp : file.list()) { //Delete files or directory under current directory.
      File fileDelete = new File(file, temp);
      deleteFileOrDirectory(fileDelete);
    }
    //Now there is nothing under directory, delete it.
    deleteFileOrDirectory(file);
  }

  public static boolean tryDeleteFileOrDirectory(File file) {
    try {
      deleteFileOrDirectory(file);
      return true;
    } catch (Exception e) {
      logger.warn("Failed to delete " + file.getAbsolutePath(), e);
      return false;
    }
  }
}
