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
import java.util.Arrays;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.OBinaryConverter;
import com.orientechnologies.common.serialization.OBinaryConverterFactory;

/**
 * Serializer for byte arrays .
 * 
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 20.01.12
 */
public class OBinaryTypeSerializer implements OBinarySerializer<byte[]> {
  private static final OBinaryConverter     CONVERTER = OBinaryConverterFactory.getConverter();

  public static final OBinaryTypeSerializer INSTANCE  = new OBinaryTypeSerializer();
  public static final byte                  ID        = 17;

  public int getObjectSize(int length) {
    return length + OIntegerSerializer.INT_SIZE;
  }

  public int getObjectSize(byte[] object, Object... hints) {
    return object.length + OIntegerSerializer.INT_SIZE;
  }

  public void serialize(byte[] object, byte[] stream, int startPosition, Object... hints) {
    int len = object.length;
    OIntegerSerializer.INSTANCE.serialize(len, stream, startPosition);
    System.arraycopy(object, 0, stream, startPosition + OIntegerSerializer.INT_SIZE, len);
  }

  public byte[] deserialize(byte[] stream, int startPosition) {
    int len = OIntegerSerializer.INSTANCE.deserialize(stream, startPosition);
    return Arrays.copyOfRange(stream, startPosition + OIntegerSerializer.INT_SIZE, startPosition + OIntegerSerializer.INT_SIZE
        + len);
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return OIntegerSerializer.INSTANCE.deserialize(stream, startPosition) + OIntegerSerializer.INT_SIZE;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return CONVERTER.getInt(stream, startPosition, ByteOrder.nativeOrder()) + OIntegerSerializer.INT_SIZE;
  }

  public void serializeNative(byte[] object, byte[] stream, int startPosition, Object... hints) {
    int len = object.length;
    CONVERTER.putInt(stream, startPosition, len, ByteOrder.nativeOrder());
    System.arraycopy(object, 0, stream, startPosition + OIntegerSerializer.INT_SIZE, len);
  }

  public byte[] deserializeNative(byte[] stream, int startPosition) {
    int len = CONVERTER.getInt(stream, startPosition, ByteOrder.nativeOrder());
    return Arrays.copyOfRange(stream, startPosition + OIntegerSerializer.INT_SIZE, startPosition + OIntegerSerializer.INT_SIZE
        + len);
  }

  @Override
  public void serializeInDirectMemory(byte[] object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    int len = object.length;
    pointer.setInt(offset, len);
    offset += OIntegerSerializer.INT_SIZE;

    pointer.set(offset, object, 0, len);
  }

  @Override
  public byte[] deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    int len = pointer.getInt(offset);
    offset += OIntegerSerializer.INT_SIZE;

    return pointer.get(offset, len);
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return pointer.getInt(offset) + OIntegerSerializer.INT_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public boolean isFixedLength() {
    return false;
  }

  public int getFixedLength() {
    return 0;
  }

  @Override
  public byte[] prepocess(byte[] value, Object... hints) {
    return value;
  }
}
