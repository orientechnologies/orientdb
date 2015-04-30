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

package com.orientechnologies.common.serialization.types.legacy;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.OBinaryConverter;
import com.orientechnologies.common.serialization.OBinaryConverterFactory;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OCharSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;

import java.nio.ByteOrder;

/**
 * Legacy serializer for {@link String} type. Keep it to support compatibility with 1.5.1.
 * 
 * 
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 18.01.12
 */
public class OStringSerializer_1_5_1 implements OBinarySerializer<String> {
  public static final OStringSerializer_1_5_1 INSTANCE  = new OStringSerializer_1_5_1();
  public static final byte                    ID        = 13;
  private static final OBinaryConverter       CONVERTER = OBinaryConverterFactory.getConverter();

  public int getObjectSize(final String object, final Object... hints) {
    return object.length() * 2 + OIntegerSerializer.INT_SIZE;
  }

  public void serialize(final String object, final byte[] stream, final int startPosition, Object... hints) {
    final OCharSerializer charSerializer = OCharSerializer.INSTANCE;
    final int length = object.length();
    OIntegerSerializer.INSTANCE.serializeLiteral(length, stream, startPosition);
    for (int i = 0; i < length; i++) {
      charSerializer.serializeLiteral(object.charAt(i), stream, startPosition + OIntegerSerializer.INT_SIZE + i * 2);
    }
  }

  public String deserialize(final byte[] stream, final int startPosition) {
    final OCharSerializer charSerializer = OCharSerializer.INSTANCE;
    final int len = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition);
    final StringBuilder stringBuilder = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      stringBuilder.append(charSerializer.deserializeLiteral(stream, startPosition + OIntegerSerializer.INT_SIZE + i * 2));
    }
    return stringBuilder.toString();
  }

  public int getObjectSize(final byte[] stream, final int startPosition) {
    return OIntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition) * 2 + OIntegerSerializer.INT_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(final byte[] stream, final int startPosition) {
    return OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition) * 2 + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public void serializeNativeObject(final String object, final byte[] stream, final int startPosition, final Object... hints) {
    int length = object.length();
    OIntegerSerializer.INSTANCE.serializeNative(length, stream, startPosition);

    int pos = startPosition + OIntegerSerializer.INT_SIZE;
    for (int i = 0; i < length; i++) {
      final char strChar = object.charAt(i);
      CONVERTER.putChar(stream, pos, strChar, ByteOrder.nativeOrder());
      pos += 2;
    }
  }

  @Override
  public String deserializeNativeObject(final byte[] stream, final int startPosition) {
    final int len = OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
    final char[] buffer = new char[len];

    int pos = startPosition + OIntegerSerializer.INT_SIZE;
    for (int i = 0; i < len; i++) {
      buffer[i] = CONVERTER.getChar(stream, pos, ByteOrder.nativeOrder());
      pos += 2;
    }
    return new String(buffer);
  }

  @Override
  public void serializeInDirectMemoryObject(final String object, final ODirectMemoryPointer pointer, long offset,
      final Object... hints) {
    final int length = object.length();
    pointer.setInt(offset, length);

    offset += OIntegerSerializer.INT_SIZE;
    for (int i = 0; i < length; i++) {
      final char strChar = object.charAt(i);
      pointer.setChar(offset, strChar);
      offset += 2;
    }
  }

  @Override
  public String deserializeFromDirectMemoryObject(final ODirectMemoryPointer pointer, long offset) {
    final int len = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(pointer, offset);
    final char[] buffer = new char[len];

    offset += OIntegerSerializer.INT_SIZE;
    for (int i = 0; i < len; i++) {
      buffer[i] = pointer.getChar(offset);
      offset += 2;
    }

    return new String(buffer);
  }

  @Override
  public String deserializeFromDirectMemoryObject(OWALChangesTree.PointerWrapper wrapper, long offset) {
    final int len = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(wrapper, offset);
    final char[] buffer = new char[len];

    offset += OIntegerSerializer.INT_SIZE;
    for (int i = 0; i < len; i++) {
      buffer[i] = wrapper.getChar(offset);
      offset += 2;
    }

    return new String(buffer);

  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return pointer.getInt(offset) * 2 + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int getObjectSizeInDirectMemory(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return wrapper.getInt(offset) * 2 + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public String preprocess(final String value, final Object... hints) {
    return value;
  }

  public boolean isFixedLength() {
    return false;
  }

  public int getFixedLength() {
    return 0;
  }
}
