package azkaban.jobtype.hiveutils;

/**
 * Simple class to represent the resulting schema of a Hive query, which may
 * or may not have been run.  We use this simple version of the results rather
 * than exposing Hive's internal classes in order to avoid tying end users
 * to any particular version of Hive or its classes.
 */
public class ResultSchema {
  final String name;
  final String type;
  final String comment;

  public ResultSchema(String name, String type, String comment) {
    this.name = name;
    this.type = type;
    this.comment = comment;
  }

  public String getComment() {
    return comment;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

}

