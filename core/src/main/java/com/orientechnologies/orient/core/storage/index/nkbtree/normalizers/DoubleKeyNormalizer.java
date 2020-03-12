package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DoubleKeyNormalizer implements KeyNormalizers {
  @Override
  public byte[] execute(Object key, final ByteOrder byteOrder, int decompositon) throws IOException {
    final ByteBuffer bb = ByteBuffer.allocate(9);
    // IEEE 754 (endian sensitive)
    bb.order(ByteOrder.BIG_ENDIAN);
    bb.put((byte) 0);
    bb.putDouble((Double) key);
    return bb.array();
  }
}
