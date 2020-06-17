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

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Serializer for byte arrays .
 *
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 20.01.12
 */
public class OBinaryTypeSerializer implements OBinarySerializer<byte[]> {
  public static final OBinaryTypeSerializer INSTANCE = new OBinaryTypeSerializer();
  public static final byte ID = 17;

  public int getObjectSize(int length) {
    return length + OIntegerSerializer.INT_SIZE;
  }

  public int getObjectSize(byte[] object, Object... hints) {
    return object.length + OIntegerSerializer.INT_SIZE;
  }

  public void serialize(
      final byte[] object, final byte[] stream, final int startPosition, final Object... hints) {
    int len = object.length;
    OIntegerSerializer.INSTANCE.serializeLiteral(len, stream, startPosition);
    System.arraycopy(object, 0, stream, startPosition + OIntegerSerializer.INT_SIZE, len);
  }

  public byte[] deserialize(final byte[] stream, final int startPosition) {
    final int len = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition);
    return Arrays.copyOfRange(
        stream,
        startPosition + OIntegerSerializer.INT_SIZE,
        startPosition + OIntegerSerializer.INT_SIZE + len);
  }

  public int getObjectSize(final byte[] stream, final int startPosition) {
    return OIntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition)
        + OIntegerSerializer.INT_SIZE;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition)
        + OIntegerSerializer.INT_SIZE;
  }

  public void serializeNativeObject(
      byte[] object, byte[] stream, int startPosition, Object... hints) {
    final int len = object.length;
    OIntegerSerializer.INSTANCE.serializeNative(len, stream, startPosition, hints);
    System.arraycopy(object, 0, stream, startPosition + OIntegerSerializer.INT_SIZE, len);
  }

  public byte[] deserializeNativeObject(byte[] stream, int startPosition) {
    final int len = OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
    return Arrays.copyOfRange(
        stream,
        startPosition + OIntegerSerializer.INT_SIZE,
        startPosition + OIntegerSerializer.INT_SIZE + len);
  }

  public byte getId() {
    return ID;
  }

  public boolean isFixedLength() {
    return false;
  }

  public int getFixedLength() {
    return 0;
  }

  @Override
  public byte[] preprocess(byte[] value, Object... hints) {
    return value;
  }

  /** {@inheritDoc} */
  @Override
  public void serializeInByteBufferObject(byte[] object, ByteBuffer buffer, Object... hints) {
    final int len = object.length;
    buffer.putInt(len);
    buffer.put(object);
  }

  /** {@inheritDoc} */
  @Override
  public byte[] deserializeFromByteBufferObject(ByteBuffer buffer) {
    final int len = buffer.getInt();
    final byte[] result = new byte[len];
    buffer.get(result);
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return buffer.getInt() + OIntegerSerializer.INT_SIZE;
  }

  /** {@inheritDoc} */
  @Override
  public byte[] deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    final int len = walChanges.getIntValue(buffer, offset);
    offset += OIntegerSerializer.INT_SIZE;
    return walChanges.getBinaryValue(buffer, offset, len);
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return walChanges.getIntValue(buffer, offset) + OIntegerSerializer.INT_SIZE;
  }
}
