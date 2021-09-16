package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * special treatment for `twos complement integer` encode as big endian unsigned integer and add
 * INT_MAX + 1 (byteswap if little endian)
 */
public final class IntegerKeyNormalizer implements KeyNormalizer {
  @Override
  public int normalizedSize(Object key) {
    return OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int normalize(Object key, int offset, byte[] stream) {
    final ByteBuffer buffer = ByteBuffer.wrap(stream);

    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.position(offset);

    buffer.putInt(((int) key) + Integer.MAX_VALUE + 1);

    return buffer.position();
  }
}
