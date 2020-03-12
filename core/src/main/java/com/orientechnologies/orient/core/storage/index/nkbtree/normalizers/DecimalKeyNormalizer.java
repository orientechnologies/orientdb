package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DecimalKeyNormalizer implements KeyNormalizers {
  @Override
  public byte[] execute(Object key, int decomposition) throws IOException {
    final BigDecimal matKey = (BigDecimal) key;
    final byte[] unscaledValue = matKey.unscaledValue().toByteArray();

    final ByteBuffer bb = ByteBuffer.allocate(1 + unscaledValue.length);
    bb.order(ByteOrder.BIG_ENDIAN);
    bb.put((byte) 0);
    // bb.putInt(matKey.scale());
    // bb.putLong(matKey.unscaledValue().longValue());
    bb.put(unscaledValue);
    return bb.array();
  }
}
