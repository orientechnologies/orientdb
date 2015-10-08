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

package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Compares types at binary level: super fast, using of literals as much as it can.
 * 
 * @author Luca Garulli
 */
public interface OBinaryComparator {
  /**
   * Compares if two values are the same.
   *
   * @param iFirstValue
   *          First value to compare
   * @param iFirstType
   *          First value type
   * @param iSecondValue
   *          Second value to compare
   * @param iSecondType
   *          Second value type
   * @return true if they match, otherwise false
   */
  boolean isEqual(BytesContainer iFirstValue, OType iFirstType, BytesContainer iSecondValue, OType iSecondType);

  /**
   * Compares two values executing also conversion between types.
   *
   * @param iValue1
   *          First value to compare
   * @param iType1
   *          First value type
   * @param iValue2
   *          Second value to compare
   * @param iType2
   *          Second value type
   * @return 0 if they matches, >0 if first value is major than second, <0 in case is minor
   */
  int compare(BytesContainer iValue1, OType iType1, BytesContainer iValue2, OType iType2);
}
