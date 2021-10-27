package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.orientechnologies.common.serialization.types.OByteSerializer;

public final class ByteKeyNormalizer implements KeyNormalizer {
  @Override
  public int normalizedSize(Object key) {
    return OByteSerializer.BYTE_SIZE;
  }

  @Override
  public int normalize(Object key, int offset, byte[] stream) {
    stream[offset] = (byte) ((byte) key + Byte.MAX_VALUE + (byte) 1);

    return offset + OByteSerializer.BYTE_SIZE;
  }
}
