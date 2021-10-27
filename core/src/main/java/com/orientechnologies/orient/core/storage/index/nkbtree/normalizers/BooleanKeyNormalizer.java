package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.orientechnologies.common.serialization.types.OByteSerializer;

public final class BooleanKeyNormalizer implements KeyNormalizer {
  @Override
  public int normalizedSize(Object key) {
    return OByteSerializer.BYTE_SIZE;
  }

  @Override
  public int normalize(Object key, int offset, byte[] stream) {
    stream[offset] = (boolean) key ? (byte) 1 : (byte) 0;

    return offset + OByteSerializer.BYTE_SIZE;
  }
}
