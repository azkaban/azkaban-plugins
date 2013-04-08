package azkaban.jobtype.hiveutils.azkaban;

import java.util.Collection;
import java.util.Properties;

public class Utils {
	public static String joinNewlines(Collection<String> strings) {
		if (strings == null || strings.size() == 0)
			return null;

		StringBuilder sb = new StringBuilder();

		for (String s : strings) {
			String trimmed = s.trim();
			sb.append(trimmed);
			if (!trimmed.endsWith("\n"))
				sb.append("\n");
		}

		return sb.toString();
	}

	// Hey, look! It's this method again! It's the freaking Where's Waldo of
	// methods...
	public static String verifyProperty(Properties p, String key) throws HiveViaAzkabanException {
		String value = p.getProperty(key);
		if (value == null) {
			throw new HiveViaAzkabanException("Can't find property " + key + " in provided Properties. Bailing");
		}
		// TODO: Add a log entry here for the value
		return value;

	}
}
