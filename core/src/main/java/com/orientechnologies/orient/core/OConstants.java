/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core;

public class OConstants {
  public static final String  ORIENT_VERSION       = "2.2.1-SNAPSHOT";

  public static final int     ORIENT_VERSION_MAJOR = 2;
  public static final int     ORIENT_VERSION_MINOR = 2;
  public static final int     ORIENT_VERSION_HOFIX = 0;
  public static final boolean SNAPSHOT             = true;

  public static final String  ORIENT_URL           = "www.orientdb.com";
  public static final String  COPYRIGHT            = "Copyrights (c) 2016 OrientDB LTD";

  /**
   * Returns the complete text of the current OrientDB version.
   */
  public static String getVersion() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(OConstants.ORIENT_VERSION);

    final String buildNumber = System.getProperty("orientdb.build.number");

    if (buildNumber != null) {
      buffer.append(" (build ");
      buffer.append(buildNumber);
      buffer.append(")");
    }

    return buffer.toString();
  }

  /**
   * Returns current OrientDB version as array with 3 integers: major, minor and hotfix numbers. Example: [2,2,0].
   */
  public static int[] getVersionNumber() {
    return new int[] { ORIENT_VERSION_MAJOR, ORIENT_VERSION_MINOR, ORIENT_VERSION_HOFIX };
  }

  /**
   * Returns true if current OrientDB version is a snapshot.
   */
  public static boolean isSnapshot() {
    return SNAPSHOT;
  }

  /**
   * Returns the build number if any.
   * 
   * @return
   */
  public static String getBuildNumber() {
    final String buildNumber = System.getProperty("orientdb.build.number");
    if (buildNumber == null)
      return null;

    return buildNumber;
  }
}
