package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class FloatKeyNormalizer implements KeyNormalizers {
  @Override
  public byte[] execute(final Object key, final ByteOrder byteOrder, int decompositon) throws IOException {
    final ByteBuffer bb = ByteBuffer.allocate(5);
    bb.order(byteOrder);
    bb.put((byte) 0);
    bb.putFloat((Float) key);
    return bb.array();
  }
}
