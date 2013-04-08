package azkaban.jobtype.hiveutils.util;

import java.lang.annotation.Documented;

/**
 * Who in LinkedIn this class is aimed at. If specified, other users may have
 * their complaints fall upon deaf ears. Caveat utilitor!
 */
@Documented
public @interface IntendedAudience {
	String value();
}
