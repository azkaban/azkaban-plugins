package azkaban.jobtype.javautils;

import org.apache.commons.lang.StringUtils;

public class ValidationUtils {

  public static void validateNotEmpty(String s, String name) {
    if(StringUtils.isEmpty(s)) {
      throw new IllegalArgumentException(name + " cannot be empty.");
    }
  }
}