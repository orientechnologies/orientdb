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

import com.orientechnologies.common.serialization.OBinaryConverter;
import com.orientechnologies.common.serialization.OBinaryConverterFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Serializer for {@link Integer} type.
 *
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 17.01.12
 */
public class OIntegerSerializer implements OBinarySerializer<Integer> {
  public static final  byte               ID        = 8;
  /**
   * size of int value in bytes
   */
  public static final  int                INT_SIZE  = 4;
  private static final OBinaryConverter   CONVERTER = OBinaryConverterFactory.getConverter();
  public static final  OIntegerSerializer INSTANCE  = new OIntegerSerializer();

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
    return (stream[startPosition]) << 24 | (0xff & stream[startPosition + 1]) << 16 | (0xff & stream[startPosition + 2]) << 8 | ((
        0xff & stream[startPosition + 3]));
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

  public void serializeNative(int object, byte[] stream, int startPosition, Object... hints) {
    CONVERTER.putInt(stream, startPosition, object, ByteOrder.nativeOrder());
  }

  public int deserializeNative(final byte[] stream, final int startPosition) {
    return CONVERTER.getInt(stream, startPosition, ByteOrder.nativeOrder());
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

  @Override
  public void serializeInByteBufferObject(Integer object, ByteBuffer buffer, Object... hints) {
    buffer.putInt(object);
  }

  @Override
  public Integer deserializeFromByteBufferObject(ByteBuffer buffer) {
    return buffer.getInt();
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return INT_SIZE;
  }

  @Override
  public Integer deserializeFromByteBufferObject(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return walChanges.getIntValue(buffer, offset);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return INT_SIZE;
  }
}
