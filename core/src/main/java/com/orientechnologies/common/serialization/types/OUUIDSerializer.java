package com.orientechnologies.common.serialization.types;

import java.util.UUID;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OUUIDSerializer implements OBinarySerializer<UUID> {
  public static final OUUIDSerializer INSTANCE  = new OUUIDSerializer();
  public static final int             UUID_SIZE = 2 * OLongSerializer.LONG_SIZE;

  @Override
  public int getObjectSize(UUID object, Object... hints) {
    return UUID_SIZE;
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return UUID_SIZE;
  }

  @Override
  public void serialize(final UUID object, final byte[] stream, final int startPosition, final Object... hints) {
    OLongSerializer.INSTANCE.serializeLiteral(object.getMostSignificantBits(), stream, startPosition);
    OLongSerializer.INSTANCE.serializeLiteral(object.getLeastSignificantBits(), stream, startPosition + OLongSerializer.LONG_SIZE);
  }

  @Override
  public UUID deserialize(byte[] stream, int startPosition) {
    final long mostSignificantBits = OLongSerializer.INSTANCE.deserializeLiteral(stream, startPosition);
    final long leastSignificantBits = OLongSerializer.INSTANCE
        .deserializeLiteral(stream, startPosition + OLongSerializer.LONG_SIZE);
    return new UUID(mostSignificantBits, leastSignificantBits);
  }

  @Override
  public byte getId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isFixedLength() {
    return OLongSerializer.INSTANCE.isFixedLength();
  }

  @Override
  public int getFixedLength() {
    return UUID_SIZE;
  }

  @Override
  public void serializeNativeObject(final UUID object, final byte[] stream, final int startPosition, final Object... hints) {
    OLongSerializer.INSTANCE.serializeNative(object.getMostSignificantBits(), stream, startPosition, hints);
    OLongSerializer.INSTANCE.serializeNative(object.getLeastSignificantBits(), stream, startPosition + OLongSerializer.LONG_SIZE,
        hints);
  }

  @Override
  public UUID deserializeNativeObject(final byte[] stream, final int startPosition) {
    final long mostSignificantBits = OLongSerializer.INSTANCE.deserializeNative(stream, startPosition);
    final long leastSignificantBits = OLongSerializer.INSTANCE.deserializeNative(stream, startPosition + OLongSerializer.LONG_SIZE);
    return new UUID(mostSignificantBits, leastSignificantBits);
  }

  @Override
  public int getObjectSizeNative(final byte[] stream, final int startPosition) {
    return UUID_SIZE;
  }

  @Override
  public void serializeInDirectMemoryObject(UUID object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    OLongSerializer.INSTANCE.serializeInDirectMemory(object.getMostSignificantBits(), pointer, offset, hints);
    OLongSerializer.INSTANCE.serializeInDirectMemory(object.getLeastSignificantBits(), pointer, offset + OLongSerializer.LONG_SIZE,
        hints);
  }

  @Override
  public UUID deserializeFromDirectMemoryObject(final ODirectMemoryPointer pointer, final long offset) {
    final long mostSignificantBits = OLongSerializer.INSTANCE.deserializeFromDirectMemory(pointer, offset);
    final long leastSignificantBits = OLongSerializer.INSTANCE.deserializeFromDirectMemory(pointer, offset
        + OLongSerializer.LONG_SIZE);
    return new UUID(mostSignificantBits, leastSignificantBits);
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return UUID_SIZE;
  }

  @Override
  public UUID preprocess(UUID value, Object... hints) {
    return value;
  }
}
