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
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 18.01.12
 */
public class OCharSerializer implements OBinarySerializer<Character> {
  /**
   * size of char value in bytes
   */
  public static final int               CHAR_SIZE        = 2;
  public static final byte              ID               = 3;
  private static final OBinaryConverter BINARY_CONVERTER = OBinaryConverterFactory.getConverter();
  public static OCharSerializer         INSTANCE         = new OCharSerializer();

  public int getObjectSize(final Character object, Object... hints) {
    return CHAR_SIZE;
  }

  public void serialize(final Character object, final byte[] stream, final int startPosition, final Object... hints) {
    serializeLiteral(object.charValue(), stream, startPosition);
  }

  public void serializeLiteral(final char value, final byte[] stream, final int startPosition) {
    stream[startPosition] = (byte) (value >>> 8);
    stream[startPosition + 1] = (byte) (value);
  }

  public Character deserialize(final byte[] stream, final int startPosition) {
    return deserializeLiteral(stream, startPosition);
  }

  public char deserializeLiteral(final byte[] stream, final int startPosition) {
    return (char) (((stream[startPosition] & 0xFF) << 8) + (stream[startPosition + 1] & 0xFF));
  }

  public int getObjectSize(final byte[] stream, final int startPosition) {
    return CHAR_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return CHAR_SIZE;
  }

  @Override
  public void serializeNativeObject(Character object, byte[] stream, int startPosition, Object... hints) {
    BINARY_CONVERTER.putChar(stream, startPosition, object, ByteOrder.nativeOrder());
  }

  @Override
  public Character deserializeNativeObject(final byte[] stream, final int startPosition) {
    return BINARY_CONVERTER.getChar(stream, startPosition, ByteOrder.nativeOrder());
  }

  public void serializeNative(final char object, final byte[] stream, final int startPosition, final Object... hints) {
    BINARY_CONVERTER.putChar(stream, startPosition, object, ByteOrder.nativeOrder());
  }

  public char deserializeNative(final byte[] stream, final int startPosition) {
    return BINARY_CONVERTER.getChar(stream, startPosition, ByteOrder.nativeOrder());
  }

  public void serializeInDirectMemory(final char object, final ODirectMemoryPointer pointer, final long offset,
      final Object... hints) {
    pointer.setChar(offset, object);
  }

  public Character deserializeFromDirectMemory(final ODirectMemoryPointer pointer, final long offset) {
    return pointer.getChar(offset);
  }

  @Override
  public void serializeInDirectMemoryObject(final Character object, final ODirectMemoryPointer pointer, final long offset,
      final Object... hints) {
    pointer.setChar(offset, object);
  }

  @Override
  public Character deserializeFromDirectMemoryObject(final ODirectMemoryPointer pointer, final long offset) {
    return pointer.getChar(offset);
  }

  @Override
  public Character deserializeFromDirectMemoryObject(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return (char) wrapper.getShort(offset);
  }

  @Override
  public int getObjectSizeInDirectMemory(final ODirectMemoryPointer pointer, final long offset) {
    return CHAR_SIZE;
  }

  @Override
  public int getObjectSizeInDirectMemory(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return CHAR_SIZE;
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return CHAR_SIZE;
  }

  @Override
  public Character preprocess(final Character value, final Object... hints) {
    return value;
  }
}
