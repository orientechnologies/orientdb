package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * special treatment for `twos complement integer`
 * encode as big endian unsigned integer and add INT_MAX + 1
 * (byteswap if little endian)
 */
public class IntegerKeyNormalizer implements KeyNormalizers {
  @Override
  public byte[] execute(final Object key, final ByteOrder byteOrder, int decompositon) throws IOException {
    final int keyVal = (int) key;
    final int uint = (int) (keyVal & 0xFFFFFFFF);
    final int i = (int)(uint + (Integer.MAX_VALUE + 1));

    //final long uint = Integer.toUnsignedLong((Integer) key);
    //final ByteBuffer byteBuffer = ByteBuffer.allocate(1).order(ByteOrder.BIG_ENDIAN).put((byte) 0).putInt(uint);
    //return (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) ? byteBuffer.flip()..array() : byteBuffer.array();
     final ByteBuffer bb = ByteBuffer.allocate(5);
     bb.order(byteOrder);
     bb.put((byte) 0);
     bb.putInt((Integer) key);
     return bb.array();
  }
}
