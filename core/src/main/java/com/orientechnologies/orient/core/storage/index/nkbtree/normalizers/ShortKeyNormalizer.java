package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.orientechnologies.common.serialization.types.OShortSerializer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class ShortKeyNormalizer implements KeyNormalizer {
  @Override
  public int normalizedSize(Object key) {
    return OShortSerializer.SHORT_SIZE;
  }

  @Override
  public int normalize(Object key, int offset, byte[] stream) {
    final ByteBuffer buffer = ByteBuffer.wrap(stream);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.position(offset);

    buffer.putShort((short) ((short) key + Short.MAX_VALUE + 1));

    return buffer.position();
  }
}
