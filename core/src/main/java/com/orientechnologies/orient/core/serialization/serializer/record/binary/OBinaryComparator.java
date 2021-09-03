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

package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Compares types at binary level: super fast, using of literals as much as it can.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface OBinaryComparator {
  /**
   * Compares if two binary values are the same.
   *
   * @param iFirstValue First value to compare
   * @param iSecondValue Second value to compare
   * @return true if they match, otherwise false
   */
  boolean isEqual(OBinaryField iFirstValue, OBinaryField iSecondValue);

  /**
   * Compares two binary values executing also conversion between types.
   *
   * @param iValue1 First value to compare
   * @param iValue2 Second value to compare
   * @return 0 if they matches, >0 if first value is major than second, <0 in case is minor
   */
  int compare(OBinaryField iValue1, OBinaryField iValue2);

  /**
   * Returns true if the type is binary comparable
   *
   * @param iType
   * @return
   */
  boolean isBinaryComparable(OType iType);
}
