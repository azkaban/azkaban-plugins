package azkaban.jobtype.hiveutils.azkaban;

public interface HiveAction {
  public void execute() throws HiveViaAzkabanException;
}

