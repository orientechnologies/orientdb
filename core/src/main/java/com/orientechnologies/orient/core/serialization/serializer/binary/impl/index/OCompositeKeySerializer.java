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
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.ONullSerializer;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Serializer that is used for serialization of {@link OCompositeKey} keys in index.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 29.07.11
 */
public class OCompositeKeySerializer implements OBinarySerializer<OCompositeKey> {

  public static final OCompositeKeySerializer INSTANCE = new OCompositeKeySerializer();
  public static final byte ID = 14;

  public int getObjectSize(OCompositeKey compositeKey, Object... hints) {
    final OType[] types = getKeyTypes(hints);

    final List<Object> keys = compositeKey.getKeys();

    int size = 2 * OIntegerSerializer.INT_SIZE;

    final OBinarySerializerFactory factory = OBinarySerializerFactory.getInstance();
    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);

      if (key != null) {
        final OType type;
        if (types.length > i) type = types[i];
        else type = OType.getTypeByClass(key.getClass());

        size +=
            OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE
                + factory.getObjectSerializer(type).getObjectSize(key);
      } else {
        size +=
            OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE
                + ONullSerializer.INSTANCE.getObjectSize(null);
      }
    }

    return size;
  }

  public void serialize(
      OCompositeKey compositeKey, byte[] stream, int startPosition, Object... hints) {
    final OType[] types = getKeyTypes(hints);

    final List<Object> keys = compositeKey.getKeys();
    final int keysSize = keys.size();

    final int oldStartPosition = startPosition;

    startPosition += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeLiteral(keysSize, stream, startPosition);

    startPosition += OIntegerSerializer.INT_SIZE;

    final OBinarySerializerFactory factory = OBinarySerializerFactory.getInstance();

    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);

      OBinarySerializer<Object> binarySerializer;
      if (key != null) {
        final OType type;
        if (types.length > i) type = types[i];
        else type = OType.getTypeByClass(key.getClass());

        binarySerializer = factory.getObjectSerializer(type);
      } else binarySerializer = ONullSerializer.INSTANCE;

      stream[startPosition] = binarySerializer.getId();
      startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      binarySerializer.serialize(key, stream, startPosition);
      startPosition += binarySerializer.getObjectSize(key);
    }

    OIntegerSerializer.INSTANCE.serializeLiteral(
        (startPosition - oldStartPosition), stream, oldStartPosition);
  }

  @SuppressWarnings("unchecked")
  public OCompositeKey deserialize(byte[] stream, int startPosition) {
    final OCompositeKey compositeKey = new OCompositeKey();

    startPosition += OIntegerSerializer.INT_SIZE;

    final int keysSize = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition);
    startPosition += OIntegerSerializer.INSTANCE.getObjectSize(keysSize);

    final OBinarySerializerFactory factory = OBinarySerializerFactory.getInstance();
    for (int i = 0; i < keysSize; i++) {
      final byte serializerId = stream[startPosition];
      startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      OBinarySerializer<Object> binarySerializer =
          (OBinarySerializer<Object>) factory.getObjectSerializer(serializerId);
      final Object key = binarySerializer.deserialize(stream, startPosition);
      compositeKey.addKey(key);

      startPosition += binarySerializer.getObjectSize(key);
    }

    return compositeKey;
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return OIntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition);
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
  }

  public void serializeNativeObject(
      OCompositeKey compositeKey, byte[] stream, int startPosition, Object... hints) {
    final OType[] types = getKeyTypes(hints);

    final List<Object> keys = compositeKey.getKeys();
    final int keysSize = keys.size();

    final int oldStartPosition = startPosition;

    startPosition += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(keysSize, stream, startPosition);

    startPosition += OIntegerSerializer.INT_SIZE;

    final OBinarySerializerFactory factory = OBinarySerializerFactory.getInstance();

    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);
      OBinarySerializer<Object> binarySerializer;
      if (key != null) {
        final OType type;
        if (types.length > i) type = types[i];
        else type = OType.getTypeByClass(key.getClass());

        binarySerializer = factory.getObjectSerializer(type);
      } else binarySerializer = ONullSerializer.INSTANCE;

      stream[startPosition] = binarySerializer.getId();
      startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      binarySerializer.serializeNativeObject(key, stream, startPosition);
      startPosition += binarySerializer.getObjectSize(key);
    }

    OIntegerSerializer.INSTANCE.serializeNative(
        (startPosition - oldStartPosition), stream, oldStartPosition);
  }

  public OCompositeKey deserializeNativeObject(byte[] stream, int startPosition) {
    final OCompositeKey compositeKey = new OCompositeKey();

    startPosition += OIntegerSerializer.INT_SIZE;

    final int keysSize = OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
    startPosition += OIntegerSerializer.INSTANCE.getObjectSize(keysSize);

    final OBinarySerializerFactory factory = OBinarySerializerFactory.getInstance();
    for (int i = 0; i < keysSize; i++) {
      final byte serializerId = stream[startPosition];
      startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      @SuppressWarnings("unchecked")
      OBinarySerializer<Object> binarySerializer =
          (OBinarySerializer<Object>) factory.getObjectSerializer(serializerId);
      final Object key = binarySerializer.deserializeNativeObject(stream, startPosition);
      compositeKey.addKey(key);

      startPosition += binarySerializer.getObjectSize(key);
    }

    return compositeKey;
  }

  private OType[] getKeyTypes(Object[] hints) {
    final OType[] types;

    if (hints != null && hints.length > 0) types = (OType[]) hints;
    else types = OCommonConst.EMPTY_TYPES_ARRAY;
    return types;
  }

  public boolean isFixedLength() {
    return false;
  }

  public int getFixedLength() {
    return 0;
  }

  @Override
  public OCompositeKey preprocess(OCompositeKey value, Object... hints) {
    if (value == null) {
      return null;
    }

    final OType[] types = getKeyTypes(hints);

    final List<Object> keys = value.getKeys();
    final OCompositeKey compositeKey = new OCompositeKey();

    final OBinarySerializerFactory factory = OBinarySerializerFactory.getInstance();
    for (int i = 0; i < keys.size(); i++) {
      Object key = keys.get(i);

      if (key != null) {
        final OType type;
        if (types.length > i) type = types[i];
        else {
          type = OType.getTypeByClass(key.getClass());
        }

        OBinarySerializer<Object> keySerializer = factory.getObjectSerializer(type);
        if (key instanceof Map
            && !(type == OType.EMBEDDEDMAP || type == OType.LINKMAP)
            && ((Map) key).size() == 1
            && ((Map) key)
                .keySet()
                .iterator()
                .next()
                .getClass()
                .isAssignableFrom(type.getDefaultJavaType())) {
          key = ((Map) key).keySet().iterator().next();
        }
        compositeKey.addKey(keySerializer.preprocess(key));
      } else {
        compositeKey.addKey(key);
      }
    }

    return compositeKey;
  }

  /** {@inheritDoc} */
  @Override
  public void serializeInByteBufferObject(
      OCompositeKey object, ByteBuffer buffer, Object... hints) {
    final OType[] types = getKeyTypes(hints);

    final List<Object> keys = object.getKeys();
    final int keysSize = keys.size();

    final int oldStartOffset = buffer.position();
    buffer.position(oldStartOffset + OIntegerSerializer.INT_SIZE);

    buffer.putInt(keysSize);
    final OBinarySerializerFactory factory = OBinarySerializerFactory.getInstance();

    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);

      OBinarySerializer<Object> binarySerializer;
      if (key != null) {
        final OType type;
        if (types.length > i) type = types[i];
        else type = OType.getTypeByClass(key.getClass());

        binarySerializer = factory.getObjectSerializer(type);
      } else binarySerializer = ONullSerializer.INSTANCE;

      buffer.put(binarySerializer.getId());
      binarySerializer.serializeInByteBufferObject(key, buffer);
    }

    final int finalPosition = buffer.position();
    final int serializedSize = buffer.position() - oldStartOffset;

    buffer.position(oldStartOffset);
    buffer.putInt(serializedSize);

    buffer.position(finalPosition);
  }

  /** {@inheritDoc} */
  @Override
  public OCompositeKey deserializeFromByteBufferObject(ByteBuffer buffer) {
    final OCompositeKey compositeKey = new OCompositeKey();

    buffer.position(buffer.position() + OIntegerSerializer.INT_SIZE);
    final int keysSize = buffer.getInt();

    final OBinarySerializerFactory factory = OBinarySerializerFactory.getInstance();
    for (int i = 0; i < keysSize; i++) {
      final byte serializerId = buffer.get();
      @SuppressWarnings("unchecked")
      OBinarySerializer<Object> binarySerializer =
          (OBinarySerializer<Object>) factory.getObjectSerializer(serializerId);
      final Object key = binarySerializer.deserializeFromByteBufferObject(buffer);
      compositeKey.addKey(key);
    }

    return compositeKey;
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return buffer.getInt();
  }

  /** {@inheritDoc} */
  @Override
  public OCompositeKey deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    final OCompositeKey compositeKey = new OCompositeKey();

    offset += OIntegerSerializer.INT_SIZE;

    final int keysSize = walChanges.getIntValue(buffer, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final OBinarySerializerFactory factory = OBinarySerializerFactory.getInstance();
    for (int i = 0; i < keysSize; i++) {
      final byte serializerId = walChanges.getByteValue(buffer, offset);
      offset += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      @SuppressWarnings("unchecked")
      OBinarySerializer<Object> binarySerializer =
          (OBinarySerializer<Object>) factory.getObjectSerializer(serializerId);
      final Object key =
          binarySerializer.deserializeFromByteBufferObject(buffer, walChanges, offset);
      compositeKey.addKey(key);

      offset += binarySerializer.getObjectSize(key);
    }

    return compositeKey;
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return walChanges.getIntValue(buffer, offset);
  }
}
