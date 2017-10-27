package com.orientechnologies.orient.core;

import com.orientechnologies.common.log.OLogManager;
import com.sun.corba.se.impl.util.ORBProperties;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class OConstants {
  public static final String ORIENT_URL = "https://www.orientdb.com";
  public static final String COPYRIGHT  = "Copyrights (c) 2017 OrientDB LTD";

  /**
   * @deprecated Use {@link #getVersion()} instead.
   */
  @Deprecated
  public static final String ORIENT_VERSION = "2.2.30-SNAPSHOT";

  @Deprecated
  public static final String GROUPID = "com.orientechnologies";

  @Deprecated
  public static final String ARTIFACTID = "orientdb-core";

  /**
   * @deprecated Use {@link #getBuildNumber()} instead.
   */
  @Deprecated
  public static final String REVISION             = "530f44fb671d22ef1e4e9debc381386fbcaa8bca";
  //deprecated properties
  public static final int    ORIENT_VERSION_MAJOR = 2;
  public static final int    ORIENT_VERSION_MINOR = 2;
  public static final int    ORIENT_VERSION_HOFIX = 18;

  private static final Properties properties = new Properties();

  static {
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
