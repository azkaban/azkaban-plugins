package azkaban.jobtype.hiveutils.util;

import java.lang.annotation.Documented;

/**
 * Description of parameter passed to this class via the Azkaban property to which the annotation is attached.
 */
@Documented
public @interface AzkabanJobPropertyDescription {
  // @TODO: Actually add the value in since it doesn't show up in the javadoc... siargh.
  String value();
}
