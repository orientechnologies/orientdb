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

import java.io.IOException;
import java.util.List;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;

/**
 * Serializer that is used for serialization of {@link OCompositeKey} keys in index.
 * 
 * @author Andrey Lomakin
 * @since 29.07.11
 */
public class OCompositeKeySerializer implements OBinarySerializer<OCompositeKey>, OStreamSerializer {

  public static final String                  NAME     = "cks";

  public static final OCompositeKeySerializer INSTANCE = new OCompositeKeySerializer();
  public static final byte                    ID       = 14;

  @SuppressWarnings("unchecked")
  public int getObjectSize(OCompositeKey compositeKey, Object... hints) {
    final OType[] types = getKeyTypes(hints);

    final List<Object> keys = compositeKey.getKeys();

    int size = 2 * OIntegerSerializer.INT_SIZE;

    final OBinarySerializerFactory factory = OBinarySerializerFactory.INSTANCE;
    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);

      final OType type;
      if (types.length > i)
        type = types[i];
      else
        type = OType.getTypeByClass(key.getClass());

      size += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE
          + ((OBinarySerializer<Object>) factory.getObjectSerializer(type)).getObjectSize(key);
    }

    return size;
  }

  public void serialize(OCompositeKey compositeKey, byte[] stream, int startPosition, Object... hints) {
    final OType[] types = getKeyTypes(hints);

    final List<Object> keys = compositeKey.getKeys();
    final int keysSize = keys.size();

    final int oldStartPosition = startPosition;

    startPosition += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serialize(keysSize, stream, startPosition);

    startPosition += OIntegerSerializer.INT_SIZE;

    final OBinarySerializerFactory factory = OBinarySerializerFactory.INSTANCE;

    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);

      final OType type;
      if (types.length > i)
        type = types[i];
      else
        type = OType.getTypeByClass(key.getClass());

      @SuppressWarnings("unchecked")
      OBinarySerializer<Object> binarySerializer = (OBinarySerializer<Object>) factory.getObjectSerializer(type);

      stream[startPosition] = binarySerializer.getId();
      startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      binarySerializer.serialize(key, stream, startPosition);
      startPosition += binarySerializer.getObjectSize(key);
    }

    OIntegerSerializer.INSTANCE.serialize((startPosition - oldStartPosition), stream, oldStartPosition);
  }

  @SuppressWarnings("unchecked")
  public OCompositeKey deserialize(byte[] stream, int startPosition) {
    final OCompositeKey compositeKey = new OCompositeKey();

    startPosition += OIntegerSerializer.INT_SIZE;

    final int keysSize = OIntegerSerializer.INSTANCE.deserialize(stream, startPosition);
    startPosition += OIntegerSerializer.INSTANCE.getObjectSize(keysSize);

    final OBinarySerializerFactory factory = OBinarySerializerFactory.INSTANCE;
    for (int i = 0; i < keysSize; i++) {
      final byte serializerId = stream[startPosition];
      startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      OBinarySerializer<Object> binarySerializer = (OBinarySerializer<Object>) factory.getObjectSerializer(serializerId);
      final Object key = binarySerializer.deserialize(stream, startPosition);
      compositeKey.addKey(key);

      startPosition += binarySerializer.getObjectSize(key);
    }

    return compositeKey;
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return OIntegerSerializer.INSTANCE.deserialize(stream, startPosition);
  }

  public byte getId() {
    return ID;
  }

  public byte[] toStream(final Object iObject) throws IOException {
    throw new UnsupportedOperationException("CSV storage format is out of dated and is not supported.");
  }

  public Object fromStream(final byte[] iStream) throws IOException {
    final OCompositeKey compositeKey = new OCompositeKey();
    final OMemoryInputStream inputStream = new OMemoryInputStream(iStream);

    final int keysSize = inputStream.getAsInteger();
    for (int i = 0; i < keysSize; i++) {
      final byte[] keyBytes = inputStream.getAsByteArray();
      final String keyString = OBinaryProtocol.bytes2string(keyBytes);
      final int typeSeparatorPos = keyString.indexOf(',');
      final OType type = OType.valueOf(keyString.substring(0, typeSeparatorPos));
      compositeKey.addKey(ORecordSerializerStringAbstract.simpleValueFromStream(keyString.substring(typeSeparatorPos + 1), type));
    }
    return compositeKey;
  }

  public String getName() {
    return NAME;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
  }

  public void serializeNative(OCompositeKey compositeKey, byte[] stream, int startPosition, Object... hints) {
    final OType[] types = getKeyTypes(hints);

    final List<Object> keys = compositeKey.getKeys();
    final int keysSize = keys.size();

    final int oldStartPosition = startPosition;

    startPosition += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(keysSize, stream, startPosition);

    startPosition += OIntegerSerializer.INT_SIZE;

    final OBinarySerializerFactory factory = OBinarySerializerFactory.INSTANCE;

    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);

      final OType type;
      if (types.length > i)
        type = types[i];
      else
        type = OType.getTypeByClass(key.getClass());

      @SuppressWarnings("unchecked")
      OBinarySerializer<Object> binarySerializer = (OBinarySerializer<Object>) factory.getObjectSerializer(type);

      stream[startPosition] = binarySerializer.getId();
      startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      binarySerializer.serializeNative(key, stream, startPosition);
      startPosition += binarySerializer.getObjectSize(key);
    }

    OIntegerSerializer.INSTANCE.serializeNative((startPosition - oldStartPosition), stream, oldStartPosition);
  }

  public OCompositeKey deserializeNative(byte[] stream, int startPosition) {
    final OCompositeKey compositeKey = new OCompositeKey();

    startPosition += OIntegerSerializer.INT_SIZE;

    final int keysSize = OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
    startPosition += OIntegerSerializer.INSTANCE.getObjectSize(keysSize);

    final OBinarySerializerFactory factory = OBinarySerializerFactory.INSTANCE;
    for (int i = 0; i < keysSize; i++) {
      final byte serializerId = stream[startPosition];
      startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      OBinarySerializer<Object> binarySerializer = (OBinarySerializer<Object>) factory.getObjectSerializer(serializerId);
      final Object key = binarySerializer.deserializeNative(stream, startPosition);
      compositeKey.addKey(key);

      startPosition += binarySerializer.getObjectSize(key);
    }

    return compositeKey;
  }

  @Override
  public void serializeInDirectMemory(OCompositeKey compositeKey, ODirectMemoryPointer pointer, long offset, Object... hints) {
    final OType[] types = getKeyTypes(hints);

    final List<Object> keys = compositeKey.getKeys();
    final int keysSize = keys.size();

    final long oldStartOffset = offset;

    offset += OIntegerSerializer.INT_SIZE;

    pointer.setInt(offset, keysSize);

    offset += OIntegerSerializer.INT_SIZE;

    final OBinarySerializerFactory factory = OBinarySerializerFactory.INSTANCE;

    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);

      final OType type;
      if (types.length > i)
        type = types[i];
      else
        type = OType.getTypeByClass(key.getClass());

      @SuppressWarnings("unchecked")
      OBinarySerializer<Object> binarySerializer = (OBinarySerializer<Object>) factory.getObjectSerializer(type);

      pointer.setByte(offset, binarySerializer.getId());
      offset += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      binarySerializer.serializeInDirectMemory(key, pointer, offset);
      offset += binarySerializer.getObjectSize(key);
    }

    pointer.setInt(oldStartOffset, (int) (offset - oldStartOffset));
  }

  private OType[] getKeyTypes(Object[] hints) {
    final OType[] types;

    if (hints != null && hints.length > 0)
      types = (OType[]) hints;
    else
      types = new OType[0];
    return types;
  }

  @Override
  public OCompositeKey deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    final OCompositeKey compositeKey = new OCompositeKey();

    offset += OIntegerSerializer.INT_SIZE;

    final int keysSize = pointer.getInt(offset);
    offset += OIntegerSerializer.INT_SIZE;

    final OBinarySerializerFactory factory = OBinarySerializerFactory.INSTANCE;
    for (int i = 0; i < keysSize; i++) {
      final byte serializerId = pointer.getByte(offset);
      offset += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      OBinarySerializer<Object> binarySerializer = (OBinarySerializer<Object>) factory.getObjectSerializer(serializerId);
      final Object key = binarySerializer.deserializeFromDirectMemory(pointer, offset);
      compositeKey.addKey(key);

      offset += binarySerializer.getObjectSize(key);
    }

    return compositeKey;
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return pointer.getInt(offset);
  }

  public boolean isFixedLength() {
    return false;
  }

  public int getFixedLength() {
    return 0;
  }

  @Override
  public OCompositeKey prepocess(OCompositeKey value, Object... hints) {
    final OType[] types = getKeyTypes(hints);

    final List<Object> keys = value.getKeys();
    final OCompositeKey compositeKey = new OCompositeKey();

    final OBinarySerializerFactory factory = OBinarySerializerFactory.INSTANCE;
    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);

      final OType type;
      if (types.length > i)
        type = types[i];
      else
        type = OType.getTypeByClass(key.getClass());

      OBinarySerializer<Object> keySerializer = ((OBinarySerializer<Object>) factory.getObjectSerializer(type));
      compositeKey.addKey(keySerializer.prepocess(key));
    }

    return compositeKey;
  }
}
