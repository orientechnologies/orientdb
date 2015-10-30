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
  public static final String ORIENT_VERSION = "2.1.5-SNAPSHOT";

  public static final String ORIENT_URL     = "www.orientdb.com";
  public static final String COPYRIGHT      = "Copyrights (c) 2015 Orient Technologies LTD";

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

  public static String getBuildNumber() {
    final String buildNumber = System.getProperty("orientdb.build.number");
    if (buildNumber == null)
      return null;

    return buildNumber;
  }
}
