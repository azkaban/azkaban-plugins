package azkaban.jobtype.javautils;

import java.util.Arrays;
import java.util.Objects;

import org.apache.commons.lang.StringUtils;

import azkaban.utils.Props;

public class ValidationUtils {

  public static void validateNotEmpty(String s, String name) {
    if(StringUtils.isEmpty(s)) {
      throw new IllegalArgumentException(name + " cannot be empty.");
    }
  }

  /**
   * Validates if all of the keys exist of none of them exist
   * @param props
   * @param keys
   * @throws IllegalArgumentException only if some of the keys exist
   */
  public static void validateAllOrNone(Props props, String... keys) {
    Objects.requireNonNull(keys);

    boolean allExist = true;
    boolean someExist = false;
    for(String key : keys) {
      Object val = props.get(key);
      allExist &= val != null;
      someExist |= val != null;
    }

    if(someExist && !allExist) {
      throw new IllegalArgumentException("Either all of properties exist or none of them should exist for " + Arrays.toString(keys));
    }
  }

  /**
   * Validates all keys present in props
   * @param props
   * @param keys
   * @throws UndefinedPropertyException if key does not exist in properties
   */
  public static void validateAllNotEmpty(Props props, String... keys) {
    for(String key : keys) {
      props.getString(key);
    }
  }
}