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
package com.orientechnologies.common.types;

/**
 * Binary wrapper to let to byte[] to be managed inside OrientDB where comparable is needed, like
 * for indexes.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 *     <p>Deprecated sice v2.2
 */
@Deprecated
public class OBinary implements Comparable<OBinary> {
  private final byte[] value;

  public OBinary(final byte[] buffer) {
    value = buffer;
  }

  public int compareTo(final OBinary o) {
    final int size = value.length;

    for (int i = 0; i < size; ++i) {
      if (value[i] > o.value[i]) return 1;
      else if (value[i] < o.value[i]) return -1;
    }
    return 0;
  }

  public byte[] toByteArray() {
    return value;
  }
}
