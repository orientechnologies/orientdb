package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DoubleKeyNormalizer implements KeyNormalizers {
  @Override
  public byte[] execute(Object key, int decomposition) throws IOException {
    final double matKey = (double) key;
    final ByteBuffer bb = ByteBuffer.allocate(9);
    // IEEE 754 (endian sensitive), positive, big-endian to match lexicographical ordering of bytes
    // for comparison
    bb.order(ByteOrder.BIG_ENDIAN);
    bb.put((byte) 0);
    bb.putLong(Double.doubleToLongBits(matKey) + Long.MAX_VALUE + 1);
    return bb.array();
  }
}
