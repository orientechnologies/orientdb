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
package com.orientechnologies.common.serialization;

import java.nio.ByteOrder;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 26.07.12
 */
public interface OBinaryConverter {
  void putInt(byte[] buffer, int index, int value, ByteOrder byteOrder);

  int getInt(byte[] buffer, int index, ByteOrder byteOrder);

  void putShort(byte[] buffer, int index, short value, ByteOrder byteOrder);

  short getShort(byte[] buffer, int index, ByteOrder byteOrder);

  void putLong(byte[] buffer, int index, long value, ByteOrder byteOrder);

  long getLong(byte[] buffer, int index, ByteOrder byteOrder);

  void putChar(byte[] buffer, int index, char character, ByteOrder byteOrder);

  char getChar(byte[] buffer, int index, ByteOrder byteOrder);

  boolean nativeAccelerationUsed();
}
