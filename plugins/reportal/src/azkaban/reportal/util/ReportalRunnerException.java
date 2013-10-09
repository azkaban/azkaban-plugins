package azkaban.reportal.util;

public class ReportalRunnerException extends Exception {
	private static final long serialVersionUID = 1L;

	public ReportalRunnerException(String s) {
		super(s);
	}

	public ReportalRunnerException(Exception e) {
		super(e);
	}

	public ReportalRunnerException(String s, Exception e) {
		super(s, e);
	}
}
