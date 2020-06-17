/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.common.serialization.types;

import com.orientechnologies.common.serialization.OBinaryConverter;
import com.orientechnologies.common.serialization.OBinaryConverterFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Serializer for {@link Short}.
 *
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 18.01.12
 */
public class OShortSerializer implements OBinarySerializer<Short> {
  public static final byte ID = 12;
  /** size of short value in bytes */
  public static final int SHORT_SIZE = 2;

  private static final OBinaryConverter CONVERTER = OBinaryConverterFactory.getConverter();
  public static final OShortSerializer INSTANCE = new OShortSerializer();

  public int getObjectSize(Short object, Object... hints) {
    return SHORT_SIZE;
  }

  public void serialize(
      final Short object, final byte[] stream, final int startPosition, final Object... hints) {
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
  public void serializeNativeObject(
      final Short object, final byte[] stream, final int startPosition, final Object... hints) {
    checkBoundaries(stream, startPosition);

    CONVERTER.putShort(stream, startPosition, object, ByteOrder.nativeOrder());
  }

  @Override
  public Short deserializeNativeObject(byte[] stream, int startPosition) {
    checkBoundaries(stream, startPosition);

    return CONVERTER.getShort(stream, startPosition, ByteOrder.nativeOrder());
  }

  public void serializeNative(
      final short object, final byte[] stream, final int startPosition, final Object... hints) {
    checkBoundaries(stream, startPosition);

    CONVERTER.putShort(stream, startPosition, object, ByteOrder.nativeOrder());
  }

  public short deserializeNative(byte[] stream, int startPosition) {
    checkBoundaries(stream, startPosition);

    return CONVERTER.getShort(stream, startPosition, ByteOrder.nativeOrder());
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

  /** {@inheritDoc} */
  @Override
  public void serializeInByteBufferObject(Short object, ByteBuffer buffer, Object... hints) {
    buffer.putShort(object);
  }

  /** {@inheritDoc} */
  @Override
  public Short deserializeFromByteBufferObject(ByteBuffer buffer) {
    return buffer.getShort();
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return SHORT_SIZE;
  }

  /** {@inheritDoc} */
  @Override
  public Short deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return walChanges.getShortValue(buffer, offset);
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return SHORT_SIZE;
  }

  private static void checkBoundaries(byte[] stream, int startPosition) {
    if (startPosition + SHORT_SIZE > stream.length) {
      throw new IllegalStateException(
          "Requested stream size is "
              + (startPosition + SHORT_SIZE)
              + " but provided stream has size "
              + stream.length);
    }
  }
}
