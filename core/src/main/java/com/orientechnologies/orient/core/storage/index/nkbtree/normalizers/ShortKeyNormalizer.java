package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ShortKeyNormalizer implements KeyNormalizers {
  @Override
  public byte[] execute(Object key, final ByteOrder byteOrder, int decompositon) throws IOException {
    final short keyVal = (Short) key;
    final short ushort = (short) (keyVal & 0xFFFF);
    final short i = (short)(ushort + Short.MAX_VALUE + 1);

    final ByteBuffer bb = ByteBuffer.allocate(3);
    bb.order(byteOrder);
    bb.put((byte) 0);
    bb.putShort((Short) key);
    return bb.array();
  }
}
