package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.orientechnologies.common.serialization.types.OLongSerializer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class DoubleKeyNormalizer implements KeyNormalizer {
  @Override
  public int normalizedSize(Object key) {
    return OLongSerializer.LONG_SIZE;
  }

  @Override
  public int normalize(Object key, int offset, byte[] stream) {
    final ByteBuffer buffer = ByteBuffer.wrap(stream);
    buffer.order(ByteOrder.BIG_ENDIAN);

    buffer.position(offset);
    buffer.putLong(Double.doubleToLongBits((double) key) + Long.MAX_VALUE + 1);

    return buffer.position();
  }
}
