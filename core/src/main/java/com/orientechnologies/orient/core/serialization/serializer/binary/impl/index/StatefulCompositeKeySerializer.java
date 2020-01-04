package com.orientechnologies.orient.core.serialization.serializer.binary.impl.index;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class StatefulCompositeKeySerializer implements OBinarySerializer<OCompositeKey> {
  private final Map<OType, OBinarySerializer<?>> definedSerializers = new ConcurrentHashMap<>();

  private volatile OBinarySerializer<Object>[] serializers;

  public void init(OType[] types) {
    if (serializers != null) {
      return;
    }

    final OBinarySerializerFactory factory = OBinarySerializerFactory.getInstance();
    @SuppressWarnings("unchecked")
    final OBinarySerializer<Object>[] serializers = new OBinarySerializer[types.length];
    for (int i = 0; i < types.length; i++) {
      OBinarySerializer<?> binarySerializer = null;
      if (!definedSerializers.isEmpty()) {
        binarySerializer = definedSerializers.get(types[i]);
      }

      if (binarySerializer == null) {
        binarySerializer = factory.getObjectSerializer(types[i]);
      }

      //noinspection unchecked
      serializers[i] = (OBinarySerializer<Object>) binarySerializer;
    }

    this.serializers = serializers;
  }

  public void defineSerializer(final OType type, final OBinarySerializer<?> serializer) {
    definedSerializers.put(type, serializer);
  }

  public int getObjectSize(OCompositeKey compositeKey, Object... hints) {
    final OType[] types = (OType[]) hints;
    final List<Object> keys = compositeKey.getKeys();

    init(types);

    int size = 2 * OIntegerSerializer.INT_SIZE;

    final OBinarySerializer<Object>[] serializers = this.serializers;
    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);
      size += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;
      if (key != null) {
        size += serializers[i].getObjectSize(key);
      }
    }

    return size;
  }

  public void serialize(OCompositeKey compositeKey, byte[] stream, int startPosition, Object... hints) {
    final OType[] types = (OType[]) hints;
    final List<Object> keys = compositeKey.getKeys();
    init(types);

    final int keysSize = keys.size();

    final int oldStartPosition = startPosition;

    startPosition += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeLiteral(keysSize, stream, startPosition);

    startPosition += OIntegerSerializer.INT_SIZE;

    final OBinarySerializer<Object>[] serializers = this.serializers;
    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);

      final OBinarySerializer<Object> binarySerializer = serializers[i];

      if (key != null) {
        stream[startPosition] = binarySerializer.getId();
        startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

        binarySerializer.serialize(key, stream, startPosition);
        startPosition += binarySerializer.getObjectSize(key);
      } else {
        stream[startPosition] = (byte) (-binarySerializer.getId() - 1);
        startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;
      }
    }

    OIntegerSerializer.INSTANCE.serializeLiteral((startPosition - oldStartPosition), stream, oldStartPosition);
  }

  @SuppressWarnings("unchecked")
  public OCompositeKey deserialize(byte[] stream, int startPosition) {
    final OCompositeKey compositeKey = new OCompositeKey();

    startPosition += OIntegerSerializer.INT_SIZE;

    final int keysSize = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition);
    startPosition += OIntegerSerializer.INSTANCE.getObjectSize(keysSize);

    OBinarySerializer<Object>[] serializers = this.serializers;
    final OBinarySerializerFactory factory;
    if (serializers == null) {
      serializers = new OBinarySerializer[keysSize];
      factory = OBinarySerializerFactory.getInstance();
    } else {
      factory = null;
    }

    assert serializers.length == keysSize;

    for (int i = 0; i < keysSize; i++) {
      final byte serializerId = stream[startPosition];
      startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      if (serializerId >= 0) {
        final OBinarySerializer<Object> binarySerializer;
        if (factory == null) {
          binarySerializer = serializers[i];
        } else {
          binarySerializer = (OBinarySerializer<Object>) factory.getObjectSerializer(serializerId);
          serializers[i] = binarySerializer;
        }

        final Object key = binarySerializer.deserialize(stream, startPosition);
        compositeKey.addKey(key);

        startPosition += binarySerializer.getObjectSize(key);
      } else {
        compositeKey.addKey(null);

        if (factory != null) {
          final OBinarySerializer<Object> binarySerializer = (OBinarySerializer<Object>) factory
              .getObjectSerializer((byte) (-serializerId - 1));
          serializers[i] = binarySerializer;
        }
      }
    }

    if (this.serializers == null) {
      this.serializers = serializers;
    }

    return compositeKey;
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return OIntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition);
  }

  public byte getId() {
    return -1;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
  }

  public void serializeNativeObject(OCompositeKey compositeKey, byte[] stream, int startPosition, Object... hints) {
    final OType[] types = (OType[]) hints;
    final List<Object> keys = compositeKey.getKeys();
    init(types);

    final int keysSize = keys.size();

    final int oldStartPosition = startPosition;

    startPosition += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(keysSize, stream, startPosition);

    startPosition += OIntegerSerializer.INT_SIZE;

    OBinarySerializer<Object>[] serializers = this.serializers;

    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);

      if (key != null) {
        final OBinarySerializer<Object> binarySerializer = serializers[i];
        stream[startPosition] = binarySerializer.getId();
        startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

        binarySerializer.serializeNativeObject(key, stream, startPosition);
        startPosition += binarySerializer.getObjectSize(key);
      } else {
        final OBinarySerializer<Object> binarySerializer = serializers[i];
        stream[startPosition] = (byte) (-binarySerializer.getId() - 1);
        startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;
      }
    }

    OIntegerSerializer.INSTANCE.serializeNative((startPosition - oldStartPosition), stream, oldStartPosition);
  }

  public OCompositeKey deserializeNativeObject(byte[] stream, int startPosition) {
    final OCompositeKey compositeKey = new OCompositeKey();

    startPosition += OIntegerSerializer.INT_SIZE;

    final int keysSize = OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
    startPosition += OIntegerSerializer.INSTANCE.getObjectSize(keysSize);

    OBinarySerializer<Object>[] serializers = this.serializers;
    final OBinarySerializerFactory factory;
    if (serializers == null) {
      //noinspection unchecked
      serializers = new OBinarySerializer[keysSize];
      factory = OBinarySerializerFactory.getInstance();
    } else {
      factory = null;
    }

    for (int i = 0; i < keysSize; i++) {
      final byte serializerId = stream[startPosition];
      startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      if (serializerId >= 0) {
        final OBinarySerializer<Object> binarySerializer;
        if (factory == null) {
          binarySerializer = serializers[i];
        } else {
          //noinspection unchecked
          binarySerializer = (OBinarySerializer<Object>) factory.getObjectSerializer(serializerId);
          serializers[i] = binarySerializer;
        }

        final Object key = binarySerializer.deserializeNativeObject(stream, startPosition);
        compositeKey.addKey(key);

        startPosition += binarySerializer.getObjectSize(key);
      } else {
        if (factory != null) {
          @SuppressWarnings("unchecked")
          final OBinarySerializer<Object> binarySerializer = (OBinarySerializer<Object>) factory
              .getObjectSerializer((byte) (-serializerId - 1));
          serializers[i] = binarySerializer;
        }
        compositeKey.addKey(null);
      }
    }

    if (this.serializers == null) {
      this.serializers = serializers;
    }

    return compositeKey;
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

    final OType[] types = (OType[]) hints;
    final List<Object> keys = value.getKeys();
    init(types);

    final OCompositeKey compositeKey = new OCompositeKey();

    final OBinarySerializer<Object>[] serializers = this.serializers;
    for (int i = 0; i < keys.size(); i++) {
      final OBinarySerializer<Object> keySerializer = serializers[i];
      final Object key = keys.get(i);
      if (key != null) {
        compositeKey.addKey(keySerializer.preprocess(key));
      } else {
        compositeKey.addKey(null);
      }
    }

    return compositeKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void serializeInByteBufferObject(OCompositeKey object, ByteBuffer buffer, Object... hints) {
    final OType[] types = (OType[]) hints;
    final List<Object> keys = object.getKeys();
    init(types);

    final int keysSize = keys.size();

    final int oldStartOffset = buffer.position();
    buffer.position(oldStartOffset + OIntegerSerializer.INT_SIZE);

    buffer.putInt(keysSize);
    final OBinarySerializer<Object>[] serializers = this.serializers;

    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);

      final OBinarySerializer<Object> binarySerializer = serializers[i];

      if (key != null) {
        buffer.put(binarySerializer.getId());
        binarySerializer.serializeInByteBufferObject(key, buffer);
      } else {
        buffer.put((byte) -(binarySerializer.getId() + 1));
      }
    }

    final int finalPosition = buffer.position();
    final int serializedSize = buffer.position() - oldStartOffset;

    buffer.position(oldStartOffset);
    buffer.putInt(serializedSize);

    buffer.position(finalPosition);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OCompositeKey deserializeFromByteBufferObject(ByteBuffer buffer) {
    final OCompositeKey compositeKey = new OCompositeKey();

    buffer.position(buffer.position() + OIntegerSerializer.INT_SIZE);
    final int keysSize = buffer.getInt();

    OBinarySerializer<Object>[] serializers = this.serializers;
    final OBinarySerializerFactory factory;
    if (serializers == null) {
      //noinspection unchecked
      serializers = new OBinarySerializer[keysSize];
      factory = OBinarySerializerFactory.getInstance();
    } else {
      factory = null;
    }

    assert serializers.length == keysSize;

    for (int i = 0; i < keysSize; i++) {
      final byte serializerId = buffer.get();
      final OBinarySerializer<Object> binarySerializer;

      if (serializerId >= 0) {
        if (factory == null) {
          binarySerializer = serializers[i];
        } else {
          //noinspection unchecked
          binarySerializer = (OBinarySerializer<Object>) factory.getObjectSerializer(serializerId);
          serializers[i] = binarySerializer;
        }

        final Object key = binarySerializer.deserializeFromByteBufferObject(buffer);
        compositeKey.addKey(key);
      } else {
        compositeKey.addKey(null);
        if (factory != null) {
          //noinspection unchecked
          binarySerializer = (OBinarySerializer<Object>) factory.getObjectSerializer((byte) (-serializerId - 1));
          serializers[i] = binarySerializer;
        }
      }
    }

    if (this.serializers == null) {
      this.serializers = serializers;
    }

    return compositeKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return buffer.getInt();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OCompositeKey deserializeFromByteBufferObject(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    final OCompositeKey compositeKey = new OCompositeKey();

    offset += OIntegerSerializer.INT_SIZE;

    final int keysSize = walChanges.getIntValue(buffer, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OBinarySerializer<Object>[] serializers = this.serializers;
    final OBinarySerializerFactory factory;
    if (serializers == null) {
      //noinspection unchecked
      serializers = new OBinarySerializer[keysSize];
      factory = OBinarySerializerFactory.getInstance();
    } else {
      factory = null;
    }

    assert serializers.length == keysSize;

    for (int i = 0; i < keysSize; i++) {
      final byte serializerId = walChanges.getByteValue(buffer, offset);
      offset += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      if (serializerId >= 0) {
        final OBinarySerializer<Object> binarySerializer;
        if (factory == null) {
          binarySerializer = serializers[i];
        } else {
          //noinspection unchecked
          binarySerializer = (OBinarySerializer<Object>) factory.getObjectSerializer(serializerId);
          serializers[i] = binarySerializer;
        }

        final Object key = binarySerializer.deserializeFromByteBufferObject(buffer, walChanges, offset);
        compositeKey.addKey(key);

        offset += binarySerializer.getObjectSize(key);
      } else {
        compositeKey.addKey(null);

        if (factory != null) {
          @SuppressWarnings("unchecked")
          final OBinarySerializer<Object> binarySerializer = (OBinarySerializer<Object>) factory
              .getObjectSerializer((byte) (-serializerId - 1));
          serializers[i] = binarySerializer;
        }
      }
    }

    if (this.serializers == null) {
      this.serializers = serializers;
    }

    return compositeKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return walChanges.getIntValue(buffer, offset);
  }
}
