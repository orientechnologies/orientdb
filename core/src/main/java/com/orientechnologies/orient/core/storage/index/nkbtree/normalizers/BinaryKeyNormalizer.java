package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.nio.ByteBuffer;

public final class BinaryKeyNormalizer implements KeyNormalizer {
  @Override
  public int normalizedSize(Object key) {
    final byte[] matKey = (byte[]) key;
    return matKey.length;
  }

  @Override
  public int normalize(Object key, int offset, byte[] stream) {
    final byte[] matKey = (byte[]) key;

    final ByteBuffer buffer = ByteBuffer.wrap(stream);
    buffer.position(offset);

    buffer.put(matKey);

    return buffer.position();
  }
}
