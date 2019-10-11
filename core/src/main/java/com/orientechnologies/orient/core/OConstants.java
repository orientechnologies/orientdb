package com.orientechnologies.orient.core;

import com.orientechnologies.common.log.OLogManager;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class OConstants {
  public static final String ORIENT_URL = "https://www.orientdb.com";
  public static final String COPYRIGHT  = "Copyrights (c) 2017 OrientDB LTD";

  /**
   * @deprecated Use {@link #getRawVersion()} instead.
   */
  @Deprecated
  public static volatile String ORIENT_VERSION;

  @Deprecated
  public static final String GROUPID = "com.orientechnologies";

  @Deprecated
  public static final String ARTIFACTID = "orientdb-core";

  /**
   * @deprecated Use {@link #getBuildNumber()} instead.
   */
  @Deprecated
  public static final String REVISION;

  /**
   * @deprecated Use {@link #getVersionMajor()} instead.
   */
  @Deprecated
  public static final int ORIENT_VERSION_MAJOR;

  /**
   * @deprecated Use {@link #getVersionMinor()} instead.
   */
  @Deprecated
  public static final int ORIENT_VERSION_MINOR;

  /**
   * @deprecated Use {@link #getVersionHotfix()} instead.
   */
  @Deprecated
  public static final int ORIENT_VERSION_HOFIX;

  private static final Properties properties = new Properties();

  static {
    try {
      final InputStream inputStream = OConstants.class.getResourceAsStream("/com/orientechnologies/orientdb.properties");
      try {
        properties.load(inputStream);
      } catch (IOException e) {
        OLogManager.instance().error(OConstants.class, "Failed to load OrientDB properties", e);
      } finally {
        if (inputStream != null) {
          try {
            inputStream.close();
          } catch (IOException ignore) {
            // Ignore
          }
        }
      }

      ORIENT_VERSION = getRawVersion();

      REVISION = getBuildNumber();

      ORIENT_VERSION_MAJOR = getVersionMajor();
      ORIENT_VERSION_MINOR = getVersionMinor();
      ORIENT_VERSION_HOFIX = getVersionHotfix();
    } catch (Exception e) {
      OLogManager.instance().errorNoDb(null, "Error during OrientDB constants initialization", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * @return Major part of OrientDB version
   */
  public static int getVersionMajor() {
    final String[] versions = properties.getProperty("version").split("\\.");
    if (versions.length == 0) {
      OLogManager.instance().errorNoDb(OConstants.class, "Can not retrieve version information for this build", null);
      return -1;
    }

    try {
      return Integer.parseInt(versions[0]);
    } catch (NumberFormatException nfe) {
      OLogManager.instance().errorNoDb(OConstants.class, "Can not retrieve major version information for this build", nfe);
      return -1;
    }
  }

  /**
   * @return Minor part of OrientDB version
   */
  public static int getVersionMinor() {
    final String[] versions = properties.getProperty("version").split("\\.");
    if (versions.length < 2) {
      OLogManager.instance().errorNoDb(OConstants.class, "Can not retrieve minor version information for this build", null);
      return -1;
    }

    try {
      return Integer.parseInt(versions[1]);
    } catch (NumberFormatException nfe) {
      OLogManager.instance().errorNoDb(OConstants.class, "Can not retrieve minor version information for this build", nfe);
      return -1;
    }
  }

  /**
   * @return Hotfix part of OrientDB version
   */
  public static int getVersionHotfix() {
    final String[] versions = properties.getProperty("version").split("\\.");
    if (versions.length < 3) {
      return 0;
    }

    try {
      String hotfix = versions[2];
      int snapshotIndex = hotfix.indexOf("-SNAPSHOT");

      if (snapshotIndex != -1) {
        hotfix = hotfix.substring(0, snapshotIndex);
      }

      return Integer.parseInt(hotfix);
    } catch (NumberFormatException nfe) {
      OLogManager.instance().errorNoDb(OConstants.class, "Can not retrieve hotfix version information for this build", nfe);
      return -1;
    }
  }

  /**
   * @return Returns only current version without build number and etc.
   */
  public static String getRawVersion() {
    return properties.getProperty("version");
  }

  /**
   * Returns the complete text of the current OrientDB version.
   */
  public static String getVersion() {
    return properties.getProperty("version") + " (build " + properties.getProperty("revision") + ", branch " + properties
        .getProperty("branch") + ")";
  }

  /**
   * Returns current OrientDB version as array with 3 integers: major, minor and hotfix numbers. Example: [3,0,0]. This method is
   * deprecated and will be removed in the future
   */
  @Deprecated
  public static int[] getVersionNumber() {
    return new int[] { ORIENT_VERSION_MAJOR, ORIENT_VERSION_MINOR, ORIENT_VERSION_HOFIX };
  }

  /**
   * Returns true if current OrientDB version is a snapshot.
   */
  public static boolean isSnapshot() {
    return properties.getProperty("version").endsWith("SNAPSHOT");
  }

  /**
   * @return the build number if any.
   */
  public static String getBuildNumber() {
    return properties.getProperty("revision");
  }
}
