package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BinaryKeyNormalizer implements KeyNormalizers {
  @Override
  public byte[] execute(Object key, ByteOrder byteOrder, int decomposition) throws IOException {
    final byte[] matKey = (byte[]) key;
    final ByteBuffer bb = ByteBuffer.allocate(1 + matKey.length);
    bb.order(byteOrder);
    bb.put((byte) 0);
    bb.put(matKey);
    return bb.array();
  }
}
