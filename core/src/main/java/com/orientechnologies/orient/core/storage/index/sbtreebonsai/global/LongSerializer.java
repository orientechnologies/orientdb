package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import java.nio.ByteBuffer;

public final class LongSerializer {

  public static int getObjectSize(long value) {
    final int zeroBits = Long.numberOfLeadingZeros(value);
    final int zerosTillFullByte = zeroBits & 7;
    final int numberSize = 8 - (zeroBits - zerosTillFullByte) / 8;
    return numberSize + 1;
  }

  public static int getObjectSize(final byte[] stream, final int offset) {
    return stream[offset] + 1;
  }

  public static int getObjectSize(final ByteBuffer buffer) {
    return buffer.get() + 1;
  }

  public static int getObjectSize(
      final ByteBuffer buffer, final OWALChanges changes, final int position) {
    return changes.getByteValue(buffer, position) + 1;
  }

  public static int serialize(long value, final byte[] stream, int position) {
    final int zeroBits = Long.numberOfLeadingZeros(value);
    final int zerosTillFullByte = zeroBits & 7;
    final int numberSize = 8 - (zeroBits - zerosTillFullByte) / 8;

    stream[position] = (byte) numberSize;
    position++;

    for (int i = 0; i < numberSize; i++) {
      stream[position + i] = (byte) ((0xFF) & value);
      value = value >>> 8;
    }

    return position + numberSize;
  }

  public static void serialize(long value, final ByteBuffer buffer) {
    final int zeroBits = Long.numberOfLeadingZeros(value);
    final int zerosTillFullByte = zeroBits & 7;
    final int numberSize = 8 - (zeroBits - zerosTillFullByte) / 8;

    buffer.put((byte) numberSize);

    for (int i = 0; i < numberSize; i++) {
      buffer.put((byte) ((0xFF) & value));
      value = value >>> 8;
    }
  }

  public static long deserialize(final ByteBuffer buffer) {
    final int numberSize = buffer.get();

    long value = 0;
    for (int i = 0; i < numberSize; i++) {
      value = value | ((0xFFL & buffer.get()) << (i * 8));
    }

    return value;
  }

  public static long deserialize(final byte[] stream, int startPosition) {
    final int numberSize = stream[startPosition];
    startPosition++;

    long value = 0;
    for (int i = 0; i < numberSize; i++) {
      value = value | ((0xFFL & stream[startPosition + i]) << (i * 8));
    }

    return value;
  }

  public static long deserialize(
      final ByteBuffer buffer, final OWALChanges changes, int startPosition) {
    final int numberSize = changes.getByteValue(buffer, startPosition);
    startPosition++;

    long value = 0;
    for (int i = 0; i < numberSize; i++) {
      value = value | ((0xFFL & changes.getByteValue(buffer, startPosition + i)) << (i * 8));
    }

    return value;
  }
}
