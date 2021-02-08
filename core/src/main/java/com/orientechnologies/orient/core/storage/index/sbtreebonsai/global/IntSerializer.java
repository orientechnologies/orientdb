package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import java.nio.ByteBuffer;

public final class IntSerializer implements OBinarySerializer<Integer> {

  public static final IntSerializer INSTANCE = new IntSerializer();

  @Override
  public int getObjectSize(Integer object, Object... hints) {
    int value = object;

    final int zeroBits = Integer.numberOfLeadingZeros(value);
    final int zerosTillFullByte = zeroBits & 7;
    final int numberSize = 4 - (zeroBits - zerosTillFullByte) / 8;

    return numberSize + 1;
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return stream[startPosition] + 1;
  }

  @Override
  public void serialize(Integer object, byte[] stream, int startPosition, Object... hints) {
    serializePrimitive(stream, startPosition, object);
  }

  @Override
  public Integer deserialize(byte[] stream, int startPosition) {
    return doDeserialize(stream, startPosition);
  }

  @Override
  public byte getId() {
    return -1;
  }

  @Override
  public boolean isFixedLength() {
    return false;
  }

  @Override
  public int getFixedLength() {
    return -1;
  }

  @Override
  public void serializeNativeObject(
      Integer object, byte[] stream, int startPosition, Object... hints) {
    serializePrimitive(stream, startPosition, object);
  }

  @Override
  public Integer deserializeNativeObject(byte[] stream, int startPosition) {
    return doDeserialize(stream, startPosition);
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return stream[startPosition] + 1;
  }

  @Override
  public Integer preprocess(final Integer value, Object... hints) {
    return value;
  }

  @Override
  public void serializeInByteBufferObject(Integer object, ByteBuffer buffer, Object... hints) {
    int value = object;

    final int zeroBits = Integer.numberOfLeadingZeros(value);
    final int zerosTillFullByte = zeroBits & 7;
    final int numberSize = 4 - (zeroBits - zerosTillFullByte) / 8;

    buffer.put((byte) numberSize);

    for (int i = 0; i < numberSize; i++) {
      buffer.put((byte) ((0xFF) & value));
      value = value >>> 8;
    }
  }

  @Override
  public Integer deserializeFromByteBufferObject(ByteBuffer buffer) {
    final int numberSize = buffer.get();

    int value = 0;
    for (int i = 0; i < numberSize; i++) {
      value = value | ((0xFF & buffer.get()) << (i * 8));
    }

    return value;
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return buffer.get() + 1;
  }

  @Override
  public Integer deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    final int numberSize = walChanges.getByteValue(buffer, offset);
    offset++;

    int value = 0;
    for (int i = 0; i < numberSize; i++) {
      value = value | ((0xFF & walChanges.getByteValue(buffer, offset)) << (i * 8));
      offset++;
    }

    return value;
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return walChanges.getByteValue(buffer, offset) + 1;
  }

  public int serializePrimitive(final byte[] stream, int startPosition, int value) {
    final int zeroBits = Integer.numberOfLeadingZeros(value);
    final int zerosTillFullByte = zeroBits & 7;
    final int numberSize = 4 - (zeroBits - zerosTillFullByte) / 8;

    stream[startPosition] = (byte) numberSize;
    startPosition++;

    for (int i = 0; i < numberSize; i++) {
      stream[startPosition + i] = (byte) ((0xFF) & value);
      value = value >>> 8;
    }

    return startPosition + numberSize;
  }

  int doDeserialize(final byte[] stream, int startPosition) {
    final int numberSize = stream[startPosition];
    startPosition++;

    int value = 0;
    for (int i = 0; i < numberSize; i++) {
      value = value | ((0xFF & stream[startPosition + i]) << (i * 8));
    }

    return value;
  }
}
