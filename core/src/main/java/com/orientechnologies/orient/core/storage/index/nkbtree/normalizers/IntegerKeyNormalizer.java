package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * special treatment for `twos complement integer` encode as big endian unsigned integer and add
 * INT_MAX + 1 (byteswap if little endian)
 */
public class IntegerKeyNormalizer implements KeyNormalizers {
  @Override
  public byte[] execute(Object key, int decomposition) throws IOException {
    final ByteBuffer bb = ByteBuffer.allocate(5);
    bb.order(ByteOrder.BIG_ENDIAN);
    bb.put((byte) 0);
    bb.putInt(((int) key) + Integer.MAX_VALUE + 1);
    return bb.array();
  }
}
