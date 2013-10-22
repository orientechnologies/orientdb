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

package com.orientechnologies.common.serialization.types;

import java.nio.ByteOrder;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.OBinaryConverter;
import com.orientechnologies.common.serialization.OBinaryConverterFactory;

/**
 * Serializer for {@link Integer} type.
 * 
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 17.01.12
 */
public class OIntegerSerializer implements OBinarySerializer<Integer> {
  private static final OBinaryConverter CONVERTER = OBinaryConverterFactory.getConverter();

  public static OIntegerSerializer      INSTANCE  = new OIntegerSerializer();
  public static final byte              ID        = 8;

  /**
   * size of int value in bytes
   */
  public static final int               INT_SIZE  = 4;

  public int getObjectSize(Integer object, Object... hints) {
    return INT_SIZE;
  }

  public void serialize(Integer object, byte[] stream, int startPosition, Object... hints) {
    final int value = object;
    stream[startPosition] = (byte) ((value >>> 24) & 0xFF);
    stream[startPosition + 1] = (byte) ((value >>> 16) & 0xFF);
    stream[startPosition + 2] = (byte) ((value >>> 8) & 0xFF);
    stream[startPosition + 3] = (byte) ((value >>> 0) & 0xFF);

  }

  public Integer deserialize(byte[] stream, int startPosition) {
    return (stream[startPosition]) << 24 | (0xff & stream[startPosition + 1]) << 16 | (0xff & stream[startPosition + 2]) << 8
        | ((0xff & stream[startPosition + 3]));
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return INT_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return INT_SIZE;
  }

  public void serializeNative(Integer object, byte[] stream, int startPosition, Object... hints) {
    CONVERTER.putInt(stream, startPosition, object, ByteOrder.nativeOrder());
  }

  public Integer deserializeNative(byte[] stream, int startPosition) {
    return CONVERTER.getInt(stream, startPosition, ByteOrder.nativeOrder());
  }

  @Override
  public void serializeInDirectMemory(Integer object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    pointer.setInt(offset, object);
  }

  @Override
  public Integer deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return pointer.getInt(offset);
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return INT_SIZE;
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return INT_SIZE;
  }

  @Override
  public Integer prepocess(Integer value, Object... hints) {
    return value;
  }
}
