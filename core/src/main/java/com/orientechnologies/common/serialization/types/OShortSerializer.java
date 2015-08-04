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
 * Serializer for {@link Short}.
 *
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 18.01.12
 */
public class OShortSerializer implements OBinarySerializer<Short> {
  public static final byte              ID         = 12;
  /**
   * size of short value in bytes
   */
  public static final int               SHORT_SIZE = 2;
  private static final OBinaryConverter CONVERTER  = OBinaryConverterFactory.getConverter();
  public static OShortSerializer        INSTANCE   = new OShortSerializer();

  public int getObjectSize(Short object, Object... hints) {
    return SHORT_SIZE;
  }

  public void serialize(final Short object, final byte[] stream, final int startPosition, final Object... hints) {
    serializeLiteral(object.shortValue(), stream, startPosition);
  }

  public void serializeLiteral(final short value, final byte[] stream, final int startPosition) {
    stream[startPosition] = (byte) ((value >>> 8) & 0xFF);
    stream[startPosition + 1] = (byte) ((value >>> 0) & 0xFF);
  }

  public Short deserialize(final byte[] stream, final int startPosition) {
    return deserializeLiteral(stream, startPosition);
  }

  public short deserializeLiteral(final byte[] stream, final int startPosition) {
    return (short) ((stream[startPosition] << 8) | (stream[startPosition + 1] & 0xff));
  }

  public int getObjectSize(final byte[] stream, final int startPosition) {
    return SHORT_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(final byte[] stream, final int startPosition) {
    return SHORT_SIZE;
  }

  @Override
  public void serializeNativeObject(final Short object, final byte[] stream, final int startPosition, final Object... hints) {
    CONVERTER.putShort(stream, startPosition, object, ByteOrder.nativeOrder());
  }

  @Override
  public Short deserializeNativeObject(byte[] stream, int startPosition) {
    return CONVERTER.getShort(stream, startPosition, ByteOrder.nativeOrder());
  }

  @Override
  public void serializeInDirectMemoryObject(Short object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    pointer.setShort(offset, object);
  }

  @Override
  public Short deserializeFromDirectMemoryObject(ODirectMemoryPointer pointer, long offset) {
    return pointer.getShort(offset);
  }

  @Override
  public Short deserializeFromDirectMemoryObject(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return wrapper.getShort(offset);
  }

  public void serializeNative(final short object, final byte[] stream, final int startPosition, final Object... hints) {
    CONVERTER.putShort(stream, startPosition, object, ByteOrder.nativeOrder());
  }

  public short deserializeNative(byte[] stream, int startPosition) {
    return CONVERTER.getShort(stream, startPosition, ByteOrder.nativeOrder());
  }

  public void serializeInDirectMemory(short object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    pointer.setShort(offset, object);
  }

  public short deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return pointer.getShort(offset);
  }

  public short deserializeFromDirectMemory(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return wrapper.getShort(offset);
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return SHORT_SIZE;
  }

  @Override
  public int getObjectSizeInDirectMemory(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return SHORT_SIZE;
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return SHORT_SIZE;
  }

  @Override
  public Short preprocess(Short value, Object... hints) {
    return value;
  }
}
