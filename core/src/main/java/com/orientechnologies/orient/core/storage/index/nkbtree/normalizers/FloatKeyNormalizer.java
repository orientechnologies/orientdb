package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class FloatKeyNormalizer implements KeyNormalizers {
  @Override
  public byte[] execute(Object key, int decomposition) throws IOException {
    final Float matKey = (float) key;
    final ByteBuffer bb = ByteBuffer.allocate(5);
    // IEEE 754 (endian sensitive), positive, big-endian to match lexicographical ordering of bytes
    // for comparison
    bb.order(ByteOrder.BIG_ENDIAN);
    bb.put((byte) 0);
    bb.putInt((Float.floatToIntBits(matKey)) + Integer.MAX_VALUE + 1);
    return bb.array();
  }
}
