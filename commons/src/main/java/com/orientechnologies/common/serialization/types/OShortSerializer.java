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
 * Serializer for {@link Short}.
 * 
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OShortSerializer implements OBinarySerializer<Short> {
  private static final OBinaryConverter CONVERTER  = OBinaryConverterFactory.getConverter();

  public static OShortSerializer        INSTANCE   = new OShortSerializer();
  public static final byte              ID         = 12;

  /**
   * size of short value in bytes
   */
  public static final int               SHORT_SIZE = 2;

  public int getObjectSize(Short object, Object... hints) {
    return SHORT_SIZE;
  }

  public void serialize(Short object, byte[] stream, int startPosition, Object... hints) {
    final short value = object;
    stream[startPosition] = (byte) ((value >>> 8) & 0xFF);
    stream[startPosition + 1] = (byte) ((value >>> 0) & 0xFF);
  }

  public Short deserialize(byte[] stream, int startPosition) {
    return (short) ((stream[startPosition] << 8) | (stream[startPosition + 1] & 0xff));
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return SHORT_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return SHORT_SIZE;
  }

  public void serializeNative(Short object, byte[] stream, int startPosition, Object... hints) {
    CONVERTER.putShort(stream, startPosition, object, ByteOrder.nativeOrder());
  }

  public Short deserializeNative(byte[] stream, int startPosition) {
    return CONVERTER.getShort(stream, startPosition, ByteOrder.nativeOrder());
  }

  @Override
  public void serializeInDirectMemory(Short object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    pointer.setShort(offset, object);
  }

  @Override
  public Short deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return pointer.getShort(offset);
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return SHORT_SIZE;
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return SHORT_SIZE;
  }

  @Override
  public Short prepocess(Short value, Object... hints) {
    return value;
  }
}
