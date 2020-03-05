package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ShortKeyNormalizer implements KeyNormalizers {
  @Override
  public byte[] execute(Object key, ByteOrder byteOrder, int decompositon) throws IOException {
    final ByteBuffer bb = ByteBuffer.allocate(3);
    bb.order(byteOrder);
    bb.put((byte) 0);
    bb.putShort((Short) key);
    return bb.array();
  }
}
