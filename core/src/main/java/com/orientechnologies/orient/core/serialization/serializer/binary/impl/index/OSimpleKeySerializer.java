/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.serialization.serializer.binary.impl.index;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;

/**
 * Serializer that is used for serialization of non {@link com.orientechnologies.common.collection.OCompositeKey} keys in index.
 * 
 * @author Andrey Lomakin
 * @since 31.03.12
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class OSimpleKeySerializer<T extends Comparable<?>> implements OBinarySerializer<T> {

  private OType              type;
  private OBinarySerializer  binarySerializer;

  public static final byte   ID   = 15;
  public static final String NAME = "bsks";

  public OSimpleKeySerializer() {
  }

  public OSimpleKeySerializer(final OType iType) {
    type = iType;
    binarySerializer = OBinarySerializerFactory.INSTANCE.getObjectSerializer(type);
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
    startPosition += binarySerializer.getObjectSize(key);
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
        + binarySerializer.getObjectSize(stream, startPosition + OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE);
  }

  public byte getId() {
    return ID;
  }

  protected void init(T key, Object[] hints) {
    if (binarySerializer == null) {
      final OType[] types;

      if (hints != null && hints.length > 0)
        types = (OType[]) hints;
      else
        types = new OType[0];

      if (types.length > 0)
        type = types[0];
      else
        type = OType.getTypeByClass(key.getClass());

      binarySerializer = OBinarySerializerFactory.INSTANCE.getObjectSerializer(type);
    }
  }

  protected void init(byte serializerId) {
    if (binarySerializer == null)
      binarySerializer = OBinarySerializerFactory.INSTANCE.getObjectSerializer(serializerId);
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    final byte serializerId = stream[startPosition];
    init(serializerId);
    return OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE
        + binarySerializer.getObjectSizeNative(stream, startPosition + OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE);
  }

  public void serializeNative(T key, byte[] stream, int startPosition, Object... hints) {
    init(key, hints);
    stream[startPosition] = binarySerializer.getId();
    startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;
    binarySerializer.serializeNative(key, stream, startPosition);
  }

  public T deserializeNative(byte[] stream, int startPosition) {
    final byte typeId = stream[startPosition];
    startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

    init(typeId);
    return (T) binarySerializer.deserializeNative(stream, startPosition);
  }

  @Override
  public void serializeInDirectMemory(T object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    init(object, hints);
    pointer.setByte(offset++, binarySerializer.getId());
    binarySerializer.serializeInDirectMemory(object, pointer, offset);
  }

  @Override
  public T deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    final byte typeId = pointer.getByte(offset++);

    init(typeId);
    return (T) binarySerializer.deserializeFromDirectMemory(pointer, offset);
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    final byte serializerId = pointer.getByte(offset);
    init(serializerId);
    return OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE
        + binarySerializer.getObjectSizeInDirectMemory(pointer, OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE + offset);

  }

  public boolean isFixedLength() {
    return binarySerializer.isFixedLength();
  }

  public int getFixedLength() {
    return binarySerializer.getFixedLength() + OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;
  }

  @Override
  public T prepocess(T value, Object... hints) {
    init(value, hints);

    return (T) binarySerializer.prepocess(value);
  }
}
