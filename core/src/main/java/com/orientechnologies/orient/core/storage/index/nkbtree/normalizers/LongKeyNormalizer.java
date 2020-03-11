package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LongKeyNormalizer implements KeyNormalizers {
  @Override
  public byte[] execute(Object key, final ByteOrder byteOrder, int decompositon) throws IOException {
    final ByteBuffer bb = ByteBuffer.allocate(9);
    // final ByteBuffer bb = ByteBuffer.allocate(8);
    bb.order(byteOrder);
    bb.put((byte) 0);
    // long matKey = (long) key;
    // bb.putLong(matKey |= Long.MIN_VALUE);
    bb.putLong((long) key);
    return bb.array();
  }
}
