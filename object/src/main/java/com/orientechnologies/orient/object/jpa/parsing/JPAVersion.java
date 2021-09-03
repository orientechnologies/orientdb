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

package com.orientechnologies.orient.object.jpa.parsing;

public enum JPAVersion {
  V1_0((byte) 1, (byte) 0),
  V2_0((byte) 2, (byte) 0),
  V2_1((byte) 2, (byte) 1);

  public final byte major;
  public final byte minor;

  JPAVersion(final byte major, final byte minor) {
    this.major = major;
    this.minor = minor;
  }

  /** @return jpa version formated as MAJOR_MINOR */
  public String getVersion() {
    return String.format("%d.%d", major, minor);
  }

  public String getFilename() {
    return String.format("persistence_%d_%d.xsd", major, minor);
  }

  public static JPAVersion parse(String version) {
    return valueOf("V" + version.charAt(0) + '_' + version.charAt(2));
  }

  @Override
  public String toString() {
    return String.format("%d_%d", major, minor);
  }
}
