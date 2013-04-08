package azkaban.jobtype.hiveutils;

public class HiveQueryException extends Exception {
  private final String query;
  private final int code;
  private final String message;

  public HiveQueryException(String query, int code, String message) {
    this.query = query;
    this.code = code;
    this.message = message;
  }

  public int getCode() {
    return code;
  }

  public String getMessage() {
    return message;
  }

  public String getQuery() {
    return query;
  }

  @Override
  public String toString() {
    return "HiveQueryException{" +
        "query='" + query + '\'' +
        ", code=" + code +
        ", message='" + message + '\'' +
        '}';
  }
}