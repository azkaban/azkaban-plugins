package azkaban.crypto;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;

public enum Version {
  V1_0("1.0"),
  V1_1("1.1");

  private static final Map<String, Version> REVERSE_ENTRIES;
  static {
    Builder<String, Version> builder = ImmutableMap.builder();
    for (Version version : Version.values()) {
      builder.put(version._ver, version);
    }
    REVERSE_ENTRIES = builder.build();
  }

  private final String _ver;

  private Version(String ver) {
    this._ver = ver;
  }

  /**
   * @return Version string
   */
  public String versionStr() {
    return _ver;
  }

  /**
   * Provides Version enum based on version String
   * @param ver Version String
   * @return
   */
  public static Version fromVerString(String ver) {
    Version result = REVERSE_ENTRIES.get(ver);
    Preconditions.checkNotNull(ver, "Invalid version " + ver);
    return result;
  }

  /**
   * @return Naturally ordered list of version String.
   */
  public static List<String> versionStrings() {
    List<String> versions = Lists.newArrayList(REVERSE_ENTRIES.keySet());
    Collections.sort(versions);
    return versions;
  }
}
