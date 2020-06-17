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

package com.orientechnologies.orient.core.record;

import com.orientechnologies.orient.core.serialization.OBinaryProtocol;

/**
 * Static helper class to manage record version.
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ORecordVersionHelper {
  public static final int SERIALIZED_SIZE = OBinaryProtocol.SIZE_INT;

  protected ORecordVersionHelper() {}

  public static int increment(final int version) {
    if (isTombstone(version))
      throw new IllegalStateException("Record was deleted and cannot be updated.");

    return version + 1;
  }

  public static int decrement(final int version) {
    if (isTombstone(version))
      throw new IllegalStateException("Record was deleted and cannot be updated.");

    return version - 1;
  }

  public static boolean isUntracked(final int version) {
    return version == -1;
  }

  public static int setRollbackMode(final int version) {
    return Integer.MIN_VALUE + version;
  }

  public static int clearRollbackMode(final int version) {
    return version - Integer.MIN_VALUE;
  }

  public static boolean isTemporary(final int version) {
    return version < -1;
  }

  public static boolean isValid(final int version) {
    return version > -1;
  }

  public static boolean isTombstone(final int version) {
    return version < 0;
  }

  public static byte[] toStream(final int version) {
    return OBinaryProtocol.int2bytes(version);
  }

  public static int fromStream(final byte[] stream) {
    return OBinaryProtocol.bytes2int(stream);
  }

  public static int reset() {
    return 0;
  }

  public static int disable() {
    return -1;
  }

  public static int compareTo(final int v1, final int v2) {
    final int myVersion;
    if (isTombstone(v1)) myVersion = -v1;
    else myVersion = v1;

    final int otherVersion;
    if (isTombstone(v2)) otherVersion = -v2;
    else otherVersion = v2;

    if (myVersion == otherVersion) return 0;

    if (myVersion < otherVersion) return -1;

    return 1;
  }

  public static String toString(final int version) {
    return String.valueOf(version);
  }

  public static int fromString(final String string) {
    return Integer.parseInt(string);
  }
}
