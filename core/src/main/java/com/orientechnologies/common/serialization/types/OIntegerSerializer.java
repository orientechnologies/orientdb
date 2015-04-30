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

package com.orientechnologies.common.serialization.types;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.OBinaryConverter;
import com.orientechnologies.common.serialization.OBinaryConverterFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;

import java.nio.ByteOrder;

/**
 * Serializer for {@link Integer} type.
 * 
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 17.01.12
 */
public class OIntegerSerializer implements OBinarySerializer<Integer> {
  public static final byte              ID        = 8;
  /**
   * size of int value in bytes
   */
  public static final int               INT_SIZE  = 4;
  private static final OBinaryConverter CONVERTER = OBinaryConverterFactory.getConverter();
  public static OIntegerSerializer      INSTANCE  = new OIntegerSerializer();

  public int getObjectSize(Integer object, Object... hints) {
    return INT_SIZE;
  }

  public void serialize(final Integer object, final byte[] stream, final int startPosition, final Object... hints) {
    serializeLiteral(object.intValue(), stream, startPosition);
  }

  public void serializeLiteral(final int value, final byte[] stream, final int startPosition) {
    stream[startPosition] = (byte) ((value >>> 24) & 0xFF);
    stream[startPosition + 1] = (byte) ((value >>> 16) & 0xFF);
    stream[startPosition + 2] = (byte) ((value >>> 8) & 0xFF);
    stream[startPosition + 3] = (byte) ((value >>> 0) & 0xFF);
  }

  public Integer deserialize(final byte[] stream, final int startPosition) {
    return deserializeLiteral(stream, startPosition);
  }

  public int deserializeLiteral(final byte[] stream, final int startPosition) {
    return (stream[startPosition]) << 24 | (0xff & stream[startPosition + 1]) << 16 | (0xff & stream[startPosition + 2]) << 8
        | ((0xff & stream[startPosition + 3]));
  }

  public int getObjectSize(final byte[] stream, final int startPosition) {
    return INT_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(final byte[] stream, final int startPosition) {
    return INT_SIZE;
  }

  @Override
  public void serializeNativeObject(Integer object, byte[] stream, int startPosition, Object... hints) {
    CONVERTER.putInt(stream, startPosition, object, ByteOrder.nativeOrder());
  }

  @Override
  public Integer deserializeNativeObject(final byte[] stream, final int startPosition) {
    return CONVERTER.getInt(stream, startPosition, ByteOrder.nativeOrder());
  }

  @Override
  public void serializeInDirectMemoryObject(final Integer object, final ODirectMemoryPointer pointer, final long offset,
      final Object... hints) {
    pointer.setInt(offset, object);
  }

  @Override
  public Integer deserializeFromDirectMemoryObject(final ODirectMemoryPointer pointer, final long offset) {
    return pointer.getInt(offset);
  }

  @Override
  public Integer deserializeFromDirectMemoryObject(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return wrapper.getInt(offset);
  }

  public void serializeNative(int object, byte[] stream, int startPosition, Object... hints) {
    CONVERTER.putInt(stream, startPosition, object, ByteOrder.nativeOrder());
  }

  public int deserializeNative(final byte[] stream, final int startPosition) {
    return CONVERTER.getInt(stream, startPosition, ByteOrder.nativeOrder());
  }

  public void serializeInDirectMemory(final int object, final ODirectMemoryPointer pointer, final long offset,
      final Object... hints) {
    pointer.setInt(offset, object);
  }

  public int deserializeFromDirectMemory(final ODirectMemoryPointer pointer, final long offset) {
    return pointer.getInt(offset);
  }

  public int deserializeFromDirectMemory(OWALChangesTree.PointerWrapper wrapper, final long offset) {
    return wrapper.getInt(offset);
  }

  @Override
  public int getObjectSizeInDirectMemory(final ODirectMemoryPointer pointer, final long offset) {
    return INT_SIZE;
  }

  @Override
  public int getObjectSizeInDirectMemory(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return INT_SIZE;
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return INT_SIZE;
  }

  @Override
  public Integer preprocess(final Integer value, final Object... hints) {
    return value;
  }
}
