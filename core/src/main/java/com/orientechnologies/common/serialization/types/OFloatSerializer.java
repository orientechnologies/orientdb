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
 * Serializer for {@link Float} type.
 *
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 18.01.12
 */
public class OFloatSerializer implements OBinarySerializer<Float> {
  public static final byte ID = 7;
  /** size of float value in bytes */
  public static final int FLOAT_SIZE = 4;

  private static final OBinaryConverter CONVERTER = OBinaryConverterFactory.getConverter();
  public static final OFloatSerializer INSTANCE = new OFloatSerializer();

  public int getObjectSize(Float object, Object... hints) {
    return FLOAT_SIZE;
  }

  public void serialize(Float object, byte[] stream, int startPosition, Object... hints) {
    OIntegerSerializer.INSTANCE.serializeLiteral(
        Float.floatToIntBits(object), stream, startPosition);
  }

  public Float deserialize(final byte[] stream, final int startPosition) {
    return Float.intBitsToFloat(
        OIntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition));
  }

  public int getObjectSize(final byte[] stream, final int startPosition) {
    return FLOAT_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return FLOAT_SIZE;
  }

  @Override
  public void serializeNativeObject(
      Float object, byte[] stream, int startPosition, Object... hints) {
    checkBoundaries(stream, startPosition);

    CONVERTER.putInt(stream, startPosition, Float.floatToIntBits(object), ByteOrder.nativeOrder());
  }

  @Override
  public Float deserializeNativeObject(byte[] stream, int startPosition) {
    checkBoundaries(stream, startPosition);

    return Float.intBitsToFloat(CONVERTER.getInt(stream, startPosition, ByteOrder.nativeOrder()));
  }

  public void serializeNative(
      final float object, final byte[] stream, final int startPosition, final Object... hints) {
    checkBoundaries(stream, startPosition);

    CONVERTER.putInt(stream, startPosition, Float.floatToIntBits(object), ByteOrder.nativeOrder());
  }

  public float deserializeNative(final byte[] stream, final int startPosition) {
    checkBoundaries(stream, startPosition);

    return Float.intBitsToFloat(CONVERTER.getInt(stream, startPosition, ByteOrder.nativeOrder()));
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return FLOAT_SIZE;
  }

  @Override
  public Float preprocess(final Float value, final Object... hints) {
    return value;
  }

  /** {@inheritDoc} */
  @Override
  public void serializeInByteBufferObject(Float object, ByteBuffer buffer, Object... hints) {
    buffer.putInt(Float.floatToIntBits(object));
  }

  /** {@inheritDoc} */
  @Override
  public Float deserializeFromByteBufferObject(ByteBuffer buffer) {
    return Float.intBitsToFloat(buffer.getInt());
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return FLOAT_SIZE;
  }

  /** {@inheritDoc} */
  @Override
  public Float deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return Float.intBitsToFloat(walChanges.getIntValue(buffer, offset));
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return FLOAT_SIZE;
  }

  private static void checkBoundaries(byte[] stream, int startPosition) {
    if (startPosition + FLOAT_SIZE > stream.length) {
      throw new IllegalStateException(
          "Requested stream size is "
              + (startPosition + FLOAT_SIZE)
              + " but provided stream has size "
              + stream.length);
    }
  }
}
