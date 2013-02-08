/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.common.serialization;

/**
 * @author Andrey Lomakin
 * @since 26.07.12
 */
public interface OBinaryConverter {
  void putInt(byte[] buffer, int index, int value);

  int getInt(byte[] buffer, int index);

  void putShort(byte[] buffer, int index, short value);

  short getShort(byte[] buffer, int index);

  void putLong(byte[] buffer, int index, long value);

  long getLong(byte[] buffer, int index);

  void putChar(byte[] buffer, int index, char character);

  char getChar(byte[] buffer, int index);

  boolean nativeAccelerationUsed();
}
