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

package com.orientechnologies.orient.core.serialization.serializer.binary.impl.index;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import java.nio.ByteBuffer;

/**
 * Serializer that is used for serialization of non {@link
 * com.orientechnologies.orient.core.index.OCompositeKey} keys in index.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 31.03.12
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class OSimpleKeySerializer<T extends Comparable<?>> implements OBinarySerializer<T> {

  private OType type;
  private OBinarySerializer binarySerializer;

  public static final byte ID = 15;
  public static final String NAME = "bsks";

  public OSimpleKeySerializer() {}

  public OSimpleKeySerializer(final OType iType) {
    type = iType;

    binarySerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(iType);
  }

  public int getObjectSize(T key, Object... hints) {
    init(key, hints);
    return OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE + binarySerializer.getObjectSize(key);
  }

  public void serialize(T key, byte[] stream, int startPosition, Object... hints) {
    init(key, hints);
    stream[startPosition] = binarySerializer.getId();
    startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;
    binarySerializer.serialize(key, stream, startPosition);
  }

  public T deserialize(byte[] stream, int startPosition) {
    final byte typeId = stream[startPosition];
    startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

    init(typeId);
    return (T) binarySerializer.deserialize(stream, startPosition);
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    final byte serializerId = stream[startPosition];
    init(serializerId);
    return OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE
        + binarySerializer.getObjectSize(
            stream, startPosition + OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE);
  }

  public byte getId() {
    return ID;
  }

  protected void init(T key, Object[] hints) {
    if (binarySerializer == null) {
      final OType[] types;

      if (hints != null && hints.length > 0) types = (OType[]) hints;
      else types = OCommonConst.EMPTY_TYPES_ARRAY;

      if (types.length > 0) type = types[0];
      else type = OType.getTypeByClass(key.getClass());

      binarySerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(type);
    }
  }

  protected void init(byte serializerId) {
    if (binarySerializer == null)
      binarySerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(serializerId);
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    final byte serializerId = stream[startPosition];
    init(serializerId);
    return OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE
        + binarySerializer.getObjectSizeNative(
            stream, startPosition + OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE);
  }

  public void serializeNativeObject(T key, byte[] stream, int startPosition, Object... hints) {
    init(key, hints);
    stream[startPosition] = binarySerializer.getId();
    startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;
    binarySerializer.serializeNativeObject(key, stream, startPosition);
  }

  public T deserializeNativeObject(byte[] stream, int startPosition) {
    final byte typeId = stream[startPosition];
    startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

    init(typeId);
    return (T) binarySerializer.deserializeNativeObject(stream, startPosition);
  }

  public boolean isFixedLength() {
    return binarySerializer.isFixedLength();
  }

  public int getFixedLength() {
    return binarySerializer.getFixedLength() + OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;
  }

  @Override
  public T preprocess(T value, Object... hints) {
    init(value, hints);

    return (T) binarySerializer.preprocess(value);
  }

  /** {@inheritDoc} */
  @Override
  public void serializeInByteBufferObject(T object, ByteBuffer buffer, Object... hints) {
    init(object, hints);
    buffer.put(binarySerializer.getId());
    binarySerializer.serializeInByteBufferObject(object, buffer);
  }

  /** {@inheritDoc} */
  @Override
  public T deserializeFromByteBufferObject(ByteBuffer buffer) {
    final byte typeId = buffer.get();

    init(typeId);
    return (T) binarySerializer.deserializeFromByteBufferObject(buffer);
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    final byte serializerId = buffer.get();
    init(serializerId);
    return OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE
        + binarySerializer.getObjectSizeInByteBuffer(buffer);
  }

  /** {@inheritDoc} */
  @Override
  public T deserializeFromByteBufferObject(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    final byte typeId = walChanges.getByteValue(buffer, offset++);

    init(typeId);
    return (T) binarySerializer.deserializeFromByteBufferObject(buffer, walChanges, offset);
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE
        + binarySerializer.getObjectSizeInByteBuffer(
            buffer, walChanges, OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE + offset);
  }
}
