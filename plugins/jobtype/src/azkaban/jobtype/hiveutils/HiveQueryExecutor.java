package azkaban.jobtype.hiveutils;

import java.io.InputStream;
import java.io.PrintStream;

/**
 * Utility to execute queries against a Hive database.
 */
public interface HiveQueryExecutor {
  /**
   * Execute the specified quer(y|ies).
   *
   * @param q Query to be executed.  Queries may include \n and mutliple, ;-delimited
   *          statements. The entire string is passed to Hive.
   *
   * @throws HiveQueryExecutionException if Hive cannont execute a query.
   */
  public void executeQuery(String q) throws HiveQueryExecutionException;

  /**
   * Redirect the query execution's stdout
   *
   * @param out
   */
  public void setOut(PrintStream out);

  /**
   * Redirect the query execution's stdin
   *
   * @param in
   */
  public void setIn(InputStream in);

  /**
   * Redirect the query execution's stderr
   *
   * @param err
   */
  public void setErr(PrintStream err);
}
