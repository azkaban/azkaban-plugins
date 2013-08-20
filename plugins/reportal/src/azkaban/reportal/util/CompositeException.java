package azkaban.reportal.util;

import java.util.List;

public class CompositeException extends Exception {
	private static final long serialVersionUID = 1L;
	private List<? extends Exception> es;

	public CompositeException(List<? extends Exception> es) {
		this.es = es;
	}

	public List<? extends Exception> getExceptions() {
		return es;
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		boolean pastFirst = false;

		for (Exception e : es) {
			if (pastFirst) {
				str.append("\n\n");
			}

			str.append(e.toString());
			pastFirst = true;
		}

		return str.toString();
	}
}
