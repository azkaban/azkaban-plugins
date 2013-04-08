package azkaban.jobtype.hiveutils.azkaban;

import java.io.IOException;

public class HiveViaAzkabanException extends Exception {

  public HiveViaAzkabanException(String s) {
    super(s);
  }

  public HiveViaAzkabanException(Exception e) {
    super(e);
  }

  public HiveViaAzkabanException(String s, Exception e) {
    super(s, e);
  }
}
