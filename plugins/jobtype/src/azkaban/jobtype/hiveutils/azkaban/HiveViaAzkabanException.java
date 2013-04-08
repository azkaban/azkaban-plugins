package azkaban.jobtype.hiveutils.azkaban;

public class HiveViaAzkabanException extends Exception {
	private static final long serialVersionUID = 1L;

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
