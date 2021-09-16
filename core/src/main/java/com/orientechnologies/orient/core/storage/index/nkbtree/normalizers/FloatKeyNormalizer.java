package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class FloatKeyNormalizer implements KeyNormalizer {
  @Override
  public int normalizedSize(Object key) {
    return OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int normalize(Object key, int offset, byte[] stream) {
    final ByteBuffer byteBuffer = ByteBuffer.wrap(stream);
    byteBuffer.order(ByteOrder.BIG_ENDIAN);
    byteBuffer.position(offset);

    byteBuffer.putInt((Float.floatToIntBits((float) key)) + Integer.MAX_VALUE + 1);

    return byteBuffer.position();
  }
}
