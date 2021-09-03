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

/**
 * Serializer for byte type .
 *
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 18.01.12
 */
public class OByteSerializer implements OBinarySerializer<Byte> {
  /** size of byte value in bytes */
  public static final int BYTE_SIZE = 1;

  public static final byte ID = 2;
  public static final OByteSerializer INSTANCE = new OByteSerializer();

  public int getObjectSize(Byte object, Object... hints) {
    return BYTE_SIZE;
  }

  public void serialize(
      final Byte object, final byte[] stream, final int startPosition, final Object... hints) {
    stream[startPosition] = object.byteValue();
  }

  public void serializeLiteral(final byte value, final byte[] stream, final int startPosition) {
    stream[startPosition] = value;
  }

  public Byte deserialize(final byte[] stream, final int startPosition) {
    return stream[startPosition];
  }

  public byte deserializeLiteral(final byte[] stream, final int startPosition) {
    return stream[startPosition];
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return BYTE_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return getObjectSize(stream, startPosition);
  }

  @Override
  public void serializeNativeObject(
      final Byte object, final byte[] stream, final int startPosition, final Object... hints) {
    serialize(object, stream, startPosition);
  }

  public void serializeNative(byte object, byte[] stream, int startPosition, Object... hints) {
    serializeLiteral(object, stream, startPosition);
  }

  public void serializeRawBytes(final byte[] bytesToWrite, final byte[] stream, int startPosition) {
    System.arraycopy(bytesToWrite, 0, stream, startPosition, bytesToWrite.length);
  }

  @Override
  public Byte deserializeNativeObject(final byte[] stream, final int startPosition) {
    return stream[startPosition];
  }

  public byte deserializeNative(final byte[] stream, final int startPosition) {
    return stream[startPosition];
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return BYTE_SIZE;
  }

  @Override
  public Byte preprocess(Byte value, Object... hints) {
    return value;
  }

  /** {@inheritDoc} */
  @Override
  public void serializeInByteBufferObject(Byte object, ByteBuffer buffer, Object... hints) {
    buffer.put(object);
  }

  /** {@inheritDoc} */
  @Override
  public Byte deserializeFromByteBufferObject(ByteBuffer buffer) {
    return buffer.get();
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return BYTE_SIZE;
  }

  /** {@inheritDoc} */
  @Override
  public Byte deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return walChanges.getByteValue(buffer, offset);
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return BYTE_SIZE;
  }
}
