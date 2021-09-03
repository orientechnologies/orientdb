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
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 18.01.12
 */
public class OCharSerializer implements OBinarySerializer<Character> {
  /** size of char value in bytes */
  public static final int CHAR_SIZE = 2;

  public static final byte ID = 3;
  private static final OBinaryConverter BINARY_CONVERTER = OBinaryConverterFactory.getConverter();
  public static final OCharSerializer INSTANCE = new OCharSerializer();

  public int getObjectSize(final Character object, Object... hints) {
    return CHAR_SIZE;
  }

  public void serialize(
      final Character object, final byte[] stream, final int startPosition, final Object... hints) {
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
  public void serializeNativeObject(
      Character object, byte[] stream, int startPosition, Object... hints) {
    checkBoundaries(stream, startPosition);

    BINARY_CONVERTER.putChar(stream, startPosition, object, ByteOrder.nativeOrder());
  }

  @Override
  public Character deserializeNativeObject(final byte[] stream, final int startPosition) {
    checkBoundaries(stream, startPosition);

    return BINARY_CONVERTER.getChar(stream, startPosition, ByteOrder.nativeOrder());
  }

  public void serializeNative(
      final char object, final byte[] stream, final int startPosition, final Object... hints) {
    checkBoundaries(stream, startPosition);

    BINARY_CONVERTER.putChar(stream, startPosition, object, ByteOrder.nativeOrder());
  }

  public char deserializeNative(final byte[] stream, final int startPosition) {
    checkBoundaries(stream, startPosition);

    return BINARY_CONVERTER.getChar(stream, startPosition, ByteOrder.nativeOrder());
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

  /** {@inheritDoc} */
  @Override
  public void serializeInByteBufferObject(Character object, ByteBuffer buffer, Object... hints) {
    buffer.putChar(object);
  }

  /** {@inheritDoc} */
  @Override
  public Character deserializeFromByteBufferObject(ByteBuffer buffer) {
    return buffer.getChar();
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return CHAR_SIZE;
  }

  /** {@inheritDoc} */
  @Override
  public Character deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return (char) walChanges.getShortValue(buffer, offset);
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return CHAR_SIZE;
  }

  private static void checkBoundaries(byte[] stream, int startPosition) {
    if (startPosition + CHAR_SIZE > stream.length) {
      throw new IllegalStateException(
          "Requested stream size is "
              + (startPosition + CHAR_SIZE)
              + " but provided stream has size "
              + stream.length);
    }
  }
}
