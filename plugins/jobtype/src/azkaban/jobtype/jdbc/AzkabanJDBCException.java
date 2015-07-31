package azkaban.jobtype.jdbc;


public class AzkabanJDBCException extends Exception {

    public AzkabanJDBCException(String string) {
        super(string);
    }

    public AzkabanJDBCException(String string, Exception e) {
        super(string, e);
    }

}