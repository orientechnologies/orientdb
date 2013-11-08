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

  public void serialize(final String object, final byte[] stream, int startPosition, Object... hints) {
    int length = object.length();
    OIntegerSerializer.INSTANCE.serialize(length, stream, startPosition);

    startPosition += OIntegerSerializer.INT_SIZE;

    byte[] binaryData = new byte[length * 2];
    char[] stringContent = new char[length];

    object.getChars(0, length, stringContent, 0);

    int counter = 0;
    for (char character : stringContent) {
      binaryData[counter] = (byte) character;
      counter++;

      binaryData[counter] = (byte) (character >>> 8);
      counter++;
    }

    System.arraycopy(binaryData, 0, stream, startPosition, binaryData.length);
  }

  public String deserialize(final byte[] stream, int startPosition) {
    int len = OIntegerSerializer.INSTANCE.deserialize(stream, startPosition);
    char[] buffer = new char[len];

    startPosition += OIntegerSerializer.INT_SIZE;

    byte[] binaryData = new byte[buffer.length * 2];
    System.arraycopy(stream, startPosition, binaryData, 0, binaryData.length);

    for (int i = 0; i < len; i++)
      buffer[i] = (char) ((0xFF & binaryData[i << 1]) | ((0xFF & binaryData[(i << 1) + 1]) << 8));

    return new String(buffer);
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

    byte[] binaryData = new byte[length * 2];
    char[] stringContent = new char[length];

    object.getChars(0, length, stringContent, 0);

    int counter = 0;
    for (char character : stringContent) {
      binaryData[counter] = (byte) character;
      counter++;

      binaryData[counter] = (byte) (character >>> 8);
      counter++;
    }

    System.arraycopy(binaryData, 0, stream, pos, binaryData.length);
  }

  public String deserializeNative(byte[] stream, int startPosition) {
    int len = OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
    char[] buffer = new char[len];

    startPosition += OIntegerSerializer.INT_SIZE;

    byte[] binaryData = new byte[buffer.length * 2];
    System.arraycopy(stream, startPosition, binaryData, 0, binaryData.length);

    for (int i = 0; i < len; i++)
      buffer[i] = (char) ((0xFF & binaryData[i << 1]) | ((0xFF & binaryData[(i << 1) + 1]) << 8));

    return new String(buffer);
  }

  @Override
  public void serializeInDirectMemory(String object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    int length = object.length();
    pointer.setInt(offset, length);

    offset += OIntegerSerializer.INT_SIZE;

    byte[] binaryData = new byte[length * 2];
    char[] stringContent = new char[length];

    object.getChars(0, length, stringContent, 0);

    int counter = 0;
    for (char character : stringContent) {
      binaryData[counter] = (byte) character;
      counter++;

      binaryData[counter] = (byte) (character >>> 8);
      counter++;
    }

    pointer.set(offset, binaryData, 0, binaryData.length);
  }

  @Override
  public String deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    int len = pointer.getInt(offset);

    final char[] buffer = new char[len];
    offset += OIntegerSerializer.INT_SIZE;

    byte[] binaryData = pointer.get(offset, buffer.length * 2);

    for (int i = 0; i < len; i++)
      buffer[i] = (char) ((0xFF & binaryData[i << 1]) | ((0xFF & binaryData[(i << 1) + 1]) << 8));

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
