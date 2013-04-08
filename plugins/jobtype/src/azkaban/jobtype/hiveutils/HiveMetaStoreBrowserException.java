package azkaban.jobtype.hiveutils;

/**
 * Thrown when unexpected Hive metastore browsing problems come up
 */
public class HiveMetaStoreBrowserException extends Exception
{
  public HiveMetaStoreBrowserException(String msg) {
    super(msg);
  }
  
  public HiveMetaStoreBrowserException(Throwable t) {
    super(t);
  }
  
  public HiveMetaStoreBrowserException(String msg, Throwable t) {
    super(msg, t);
  }
}