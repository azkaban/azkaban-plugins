package azkaban.jobtype.hiveutils;

/**
 * Thrown when a query sent for execution ends unsuccessfully.
 */
public class HiveQueryExecutionException extends Exception {
  /**
   * Query that caused the failure.
   */
  private final String query;

  /**
   * Error code defined by Hive
   */
  private final int returnCode;

  public HiveQueryExecutionException(int returnCode, String query) {
    this.returnCode = returnCode;
    this.query = query;
  }

  public String getLine() {
    return query;
  }

  public int getReturnCode() {
    return returnCode;
  }

  @Override
  public String toString() {
    return "HiveQueryExecutionException{" +
        "query='" + query + '\'' +
        ", returnCode=" + returnCode +
        '}';
  }
}
