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

package com.orientechnologies.orient.core.version;

import com.orientechnologies.orient.core.serialization.OBinaryProtocol;

/**
 * 
 * 
 * @author Artem Orobets (enisher-at-gmail.com)
 * @since 10/25/12
 */
public class OVersionFactory {
  private static final OVersionFactory instance = new OVersionFactory();

  private static long convertMacToLong(byte[] mac) {
    long result = 0;
    for (int i = mac.length - 1; i >= 0; i--) {
      result = (result << 8) | (mac[i] & 0xFF);
    }
    return result;
  }

  public static OVersionFactory instance() {
    return instance;
  }

  public ORecordVersion createVersion() {
    return new OSimpleVersion();
  }

  public ORecordVersion createTombstone() {
    return new OSimpleVersion(-1);
  }

  public ORecordVersion createUntrackedVersion() {
    return new OSimpleVersion(-1);
  }

  public int getVersionSize() {
    return OBinaryProtocol.SIZE_INT;
  }
}
