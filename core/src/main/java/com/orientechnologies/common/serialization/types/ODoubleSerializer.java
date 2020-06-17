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
 * Serializer for {@link Double}
 *
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 17.01.12
 */
public class ODoubleSerializer implements OBinarySerializer<Double> {
  public static final byte ID = 6;
  /** size of double value in bytes */
  public static final int DOUBLE_SIZE = 8;

  private static final OBinaryConverter CONVERTER = OBinaryConverterFactory.getConverter();
  public static final ODoubleSerializer INSTANCE = new ODoubleSerializer();

  public int getObjectSize(Double object, Object... hints) {
    return DOUBLE_SIZE;
  }

  public void serialize(
      final Double object, final byte[] stream, final int startPosition, final Object... hints) {
    OLongSerializer.INSTANCE.serializeLiteral(
        Double.doubleToLongBits(object), stream, startPosition);
  }

  public Double deserialize(final byte[] stream, final int startPosition) {
    return Double.longBitsToDouble(
        OLongSerializer.INSTANCE.deserializeLiteral(stream, startPosition));
  }

  public int getObjectSize(final byte[] stream, final int startPosition) {
    return DOUBLE_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(final byte[] stream, final int startPosition) {
    return DOUBLE_SIZE;
  }

  public void serializeNative(
      final double object, final byte[] stream, final int startPosition, final Object... hints) {
    checkBoundaries(stream, startPosition);

    CONVERTER.putLong(
        stream, startPosition, Double.doubleToLongBits(object), ByteOrder.nativeOrder());
  }

  public double deserializeNative(byte[] stream, int startPosition) {
    checkBoundaries(stream, startPosition);

    return Double.longBitsToDouble(
        CONVERTER.getLong(stream, startPosition, ByteOrder.nativeOrder()));
  }

  @Override
  public void serializeNativeObject(
      final Double object, final byte[] stream, final int startPosition, final Object... hints) {
    checkBoundaries(stream, startPosition);

    CONVERTER.putLong(
        stream, startPosition, Double.doubleToLongBits(object), ByteOrder.nativeOrder());
  }

  @Override
  public Double deserializeNativeObject(byte[] stream, int startPosition) {
    checkBoundaries(stream, startPosition);

    return Double.longBitsToDouble(
        CONVERTER.getLong(stream, startPosition, ByteOrder.nativeOrder()));
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return DOUBLE_SIZE;
  }

  @Override
  public Double preprocess(final Double value, final Object... hints) {
    return value;
  }

  /** {@inheritDoc} */
  @Override
  public void serializeInByteBufferObject(Double object, ByteBuffer buffer, Object... hints) {
    buffer.putLong(Double.doubleToLongBits(object));
  }

  /** {@inheritDoc} */
  @Override
  public Double deserializeFromByteBufferObject(ByteBuffer buffer) {
    return Double.longBitsToDouble(buffer.getLong());
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return DOUBLE_SIZE;
  }

  /** {@inheritDoc} */
  @Override
  public Double deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return Double.longBitsToDouble(walChanges.getLongValue(buffer, offset));
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return DOUBLE_SIZE;
  }

  private static void checkBoundaries(byte[] stream, int startPosition) {
    if (startPosition + DOUBLE_SIZE > stream.length) {
      throw new IllegalStateException(
          "Requested stream size is "
              + (startPosition + DOUBLE_SIZE)
              + " but provided stream has size "
              + stream.length);
    }
  }
}
