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
 * Serializer for {@link String} type.
 * 
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OStringSerializer implements OBinarySerializer<String> {
  private static final OBinaryConverter CONVERTER = OBinaryConverterFactory.getConverter();

  public static final OStringSerializer INSTANCE  = new OStringSerializer();
  public static final byte              ID        = 13;

  public int getObjectSize(final String object, Object... hints) {
    return object.length() * 2 + OIntegerSerializer.INT_SIZE;
  }

  public void serialize(final String object, final byte[] stream, final int startPosition, Object... hints) {
    final OCharSerializer charSerializer = OCharSerializer.INSTANCE;
    final int length = object.length();
    OIntegerSerializer.INSTANCE.serialize(length, stream, startPosition);
    for (int i = 0; i < length; i++) {
      charSerializer.serialize(object.charAt(i), stream, startPosition + OIntegerSerializer.INT_SIZE + i * 2);
    }
  }

  public String deserialize(final byte[] stream, final int startPosition) {
    final OCharSerializer charSerializer = OCharSerializer.INSTANCE;
    final int len = OIntegerSerializer.INSTANCE.deserialize(stream, startPosition);
    final StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < len; i++) {
      stringBuilder.append(charSerializer.deserialize(stream, startPosition + OIntegerSerializer.INT_SIZE + i * 2));
    }
    return stringBuilder.toString();
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return OIntegerSerializer.INSTANCE.deserialize(stream, startPosition) * 2 + OIntegerSerializer.INT_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition) * 2 + OIntegerSerializer.INT_SIZE;
  }

  public void serializeNative(String object, byte[] stream, int startPosition, Object... hints) {
    int length = object.length();
    OIntegerSerializer.INSTANCE.serializeNative(length, stream, startPosition);

    int pos = startPosition + OIntegerSerializer.INT_SIZE;
    for (int i = 0; i < length; i++) {
      final char strChar = object.charAt(i);
      CONVERTER.putChar(stream, pos, strChar, ByteOrder.nativeOrder());
      pos += 2;
    }
  }

  public String deserializeNative(byte[] stream, int startPosition) {
    int len = OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
    char[] buffer = new char[len];

    int pos = startPosition + OIntegerSerializer.INT_SIZE;
    for (int i = 0; i < len; i++) {
      buffer[i] = CONVERTER.getChar(stream, pos, ByteOrder.nativeOrder());
      pos += 2;
    }
    return new String(buffer);
  }

  @Override
  public void serializeInDirectMemory(String object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    int length = object.length();
    pointer.setInt(offset, length);

    offset += OIntegerSerializer.INT_SIZE;
    for (int i = 0; i < length; i++) {
      final char strChar = object.charAt(i);
      pointer.setChar(offset, strChar);
      offset += 2;
    }
  }

  @Override
  public String deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    int len = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(pointer, offset);
    char[] buffer = new char[len];

    offset += OIntegerSerializer.INT_SIZE;
    for (int i = 0; i < len; i++) {
      buffer[i] = pointer.getChar(offset);
      offset += 2;
    }

    return new String(buffer);
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return pointer.getInt(offset) * 2 + OIntegerSerializer.INT_SIZE;
  }

  public boolean isFixedLength() {
    return false;
  }

  public int getFixedLength() {
    return 0;
  }

  @Override
  public String prepocess(String value, Object... hints) {
    return value;
  }
}
