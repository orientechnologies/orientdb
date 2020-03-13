package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DecimalKeyNormalizer implements KeyNormalizers {
  private static final BigInteger TWO_COMPL_REF = BigInteger.ONE.shiftLeft(64);

  @Override
  public byte[] execute(Object key, int decomposition) throws IOException {
    final BigDecimal matKey = (BigDecimal) key;
    // final BigInteger unsigned = unsigned(matKey.unscaledValue().longValue());
    // final BigInteger unscaledValue = matKey.unscaledValue().add(TWO_COMPL_REF);
    // final byte[] bytes = unscaledValue.toByteArray();

    final ByteBuffer bb = ByteBuffer.allocate(1 + 8); // bytes.length);
    bb.order(ByteOrder.BIG_ENDIAN);
    bb.put((byte) 0);
    // bb.putInt(matKey.scale());
    // bb.putLong(matKey.unscaledValue().longValue());
    // bb.put(bytes);
    bb.putLong((matKey.unscaledValue().longValue()) + Long.MAX_VALUE + 1);
    return bb.array();
  }

  BigInteger unsigned(long value) {
    return BigInteger.valueOf(value >>> 1).shiftLeft(1) // the upper 63 bits
        .or(BigInteger.valueOf(value & 1L)); // plus the lowest bit
  }
}
