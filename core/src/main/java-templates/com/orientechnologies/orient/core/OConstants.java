/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core;

public class OConstants {
  public static final String  ORIENT_URL           = "https://www.orientdb.com";
  public static final String  COPYRIGHT            = "Copyrights (c) 2017 OrientDB LTD";
  public static final String  ORIENT_VERSION       = "${project.version}";
  public static final String  GROUPID              = "${project.groupId}";
  public static final String  ARTIFACTID           = "${project.artifactId}";
  public static final String  REVISION             = "${buildNumber}";
  //deprecated properties
  public static final int     ORIENT_VERSION_MAJOR = 3;
  public static final int     ORIENT_VERSION_MINOR = 0;
  public static final int     ORIENT_VERSION_HOFIX = 0;
  public static final boolean SNAPSHOT             = false;

  /**
   * Returns the complete text of the current OrientDB version.
   */
  public static String getVersion() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(OConstants.ORIENT_VERSION);
    buffer.append(" (build ");
    buffer.append(OConstants.REVISION);
    buffer.append(")");

    return buffer.toString();
  }

  /**
   * Returns current OrientDB version as array with 3 integers: major, minor and hotfix numbers. Example: [3,0,0].
   * This method is deprecated and will be removed in the future
   */
  @Deprecated
  public static int[] getVersionNumber() {
    return new int[] { ORIENT_VERSION_MAJOR, ORIENT_VERSION_MINOR, ORIENT_VERSION_HOFIX };
  }

  /**
   * Returns true if current OrientDB version is a snapshot.
   */
  public static boolean isSnapshot() {
    return ORIENT_VERSION.endsWith("SNAPSHOT");
  }

  /**
   * Returns the build number if any.
   *
   * @return
   */
  public static String getBuildNumber() {
    return OConstants.REVISION;
  }
}
