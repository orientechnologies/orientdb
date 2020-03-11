package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Calendar;
import java.util.Date;

public class DateTimeKeyNormalizer implements KeyNormalizers {
  @Override
  public byte[] execute(final Object key, final ByteOrder byteOrder, final int decomposition) throws IOException {
    final ByteBuffer bb = ByteBuffer.allocate(9);
    bb.order(byteOrder);
    bb.put((byte) 0);
    bb.putLong(((Date) key).getTime());
    return bb.array();
  }
}
