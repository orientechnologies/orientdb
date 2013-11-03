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
 * Serializer for {@link Long} type.
 * 
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OLongSerializer implements OBinarySerializer<Long> {
  private static final OBinaryConverter CONVERTER = OBinaryConverterFactory.getConverter();

  public static OLongSerializer         INSTANCE  = new OLongSerializer();
  public static final byte              ID        = 10;

  /**
   * size of long value in bytes
   */
  public static final int               LONG_SIZE = 8;

  public int getObjectSize(Long object, Object... hints) {
    return LONG_SIZE;
  }

  public void serialize(Long object, byte[] stream, int startPosition, Object... hints) {
    final long value = object;
    stream[startPosition] = (byte) ((value >>> 56) & 0xFF);
    stream[startPosition + 1] = (byte) ((value >>> 48) & 0xFF);
    stream[startPosition + 2] = (byte) ((value >>> 40) & 0xFF);
    stream[startPosition + 3] = (byte) ((value >>> 32) & 0xFF);
    stream[startPosition + 4] = (byte) ((value >>> 24) & 0xFF);
    stream[startPosition + 5] = (byte) ((value >>> 16) & 0xFF);
    stream[startPosition + 6] = (byte) ((value >>> 8) & 0xFF);
    stream[startPosition + 7] = (byte) ((value >>> 0) & 0xFF);
  }

  public Long deserialize(byte[] stream, int startPosition) {
    return ((0xff & stream[startPosition + 7]) | (0xff & stream[startPosition + 6]) << 8 | (0xff & stream[startPosition + 5]) << 16
        | (long) (0xff & stream[startPosition + 4]) << 24 | (long) (0xff & stream[startPosition + 3]) << 32
        | (long) (0xff & stream[startPosition + 2]) << 40 | (long) (0xff & stream[startPosition + 1]) << 48 | (long) (0xff & stream[startPosition]) << 56);
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return LONG_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return LONG_SIZE;
  }

  public void serializeNative(Long object, byte[] stream, int startPosition, Object... hints) {
    CONVERTER.putLong(stream, startPosition, object, ByteOrder.nativeOrder());
  }

  public Long deserializeNative(byte[] stream, int startPosition) {
    return CONVERTER.getLong(stream, startPosition, ByteOrder.nativeOrder());
  }

  @Override
  public void serializeInDirectMemory(Long object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    pointer.setLong(offset, object);
  }

  @Override
  public Long deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return pointer.getLong(offset);
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return LONG_SIZE;
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return LONG_SIZE;
  }

  @Override
  public Long prepocess(Long value, Object... hints) {
    return value;
  }
}
