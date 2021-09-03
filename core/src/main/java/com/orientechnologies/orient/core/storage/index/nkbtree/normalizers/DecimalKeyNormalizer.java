package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DecimalKeyNormalizer implements KeyNormalizers {
  private static final BigInteger BIG_INT_TEN = new BigInteger("10");
  private static final BigInteger BIG_INT_ONE = new BigInteger("1");
  private static final BigInteger BIG_INT_ZERO = new BigInteger("0");

  private static final BigInteger TWO_COMPL_REF = BigInteger.ONE.shiftLeft(64);

  @Override
  public byte[] execute(Object key, int decomposition) throws IOException {
    final BigDecimal matKey = (BigDecimal) key;
    // decimal128 precision
    // matKey.setScale(34, RoundingMode.UP);

    // final BigInteger unsigned = unsigned(matKey.unscaledValue().longValue());
    // final BigInteger unscaledValue = matKey.unscaledValue().add(TWO_COMPL_REF);
    // final byte[] bytes = unscaledValue.toByteArray();

    final ByteBuffer bb = ByteBuffer.allocate(1 + 8); // bytes.length);
    bb.order(ByteOrder.BIG_ENDIAN);
    bb.put((byte) 0);
    // bb.putInt(matKey.scale());
    // bb.putLong(matKey.unscaledValue().longValue());
    // bb.put(bytes);
    /** NOTE: bigdecimal to double / long loses precision */
    bb.putLong(Double.doubleToLongBits(matKey.doubleValue()) + Long.MAX_VALUE + 1);
    return bb.array();
  }

  BigInteger unsigned(long value) {
    return BigInteger.valueOf(value >>> 1)
        .shiftLeft(1) // the upper 63 bits
        .or(BigInteger.valueOf(value & 1L)); // plus the lowest bit
  }

  private BigDecimal scaleToDecimal128(final BigDecimal rawValue) {
    final BigDecimal value = clampAndRound(rawValue);
    long exponent = (long) (-value.scale());
    if (exponent >= -6176L && exponent <= 6111L) {
      if (value.unscaledValue().bitLength() > 113) {
        throw new AssertionError(
            "Unscaled roundedValue is out of range for Decimal128 encoding:"
                + value.unscaledValue());
      }
    } else {
      throw new AssertionError("Exponent is out of range for Decimal128 encoding: " + exponent);
    }
    return value;
  }

  private BigDecimal clampAndRound(BigDecimal initialValue) {
    BigDecimal value;
    int diff;
    if (-initialValue.scale() > 6111) {
      diff = -initialValue.scale() - 6111;
      if (initialValue.unscaledValue().equals(BIG_INT_ZERO)) {
        value = new BigDecimal(initialValue.unscaledValue(), -6111);
      } else {
        if (diff + initialValue.precision() > 34) {
          throw new NumberFormatException(
              "Exponent is out of range for Decimal128 encoding of " + initialValue);
        }
        final BigInteger multiplier = BIG_INT_TEN.pow(diff);
        value =
            new BigDecimal(
                initialValue.unscaledValue().multiply(multiplier), initialValue.scale() + diff);
      }
    } else if (-initialValue.scale() < -6176) {
      diff = initialValue.scale() + -6176;
      int undiscardedPrecision = ensureExactRounding(initialValue, diff);
      BigInteger divisor = undiscardedPrecision == 0 ? BIG_INT_ONE : BIG_INT_TEN.pow(diff);
      value =
          new BigDecimal(initialValue.unscaledValue().divide(divisor), initialValue.scale() - diff);
    } else {
      value = initialValue.round(MathContext.DECIMAL128);
      diff = initialValue.precision() - value.precision();
      if (diff > 0) {
        ensureExactRounding(initialValue, diff);
      }
    }
    return value;
  }

  private int ensureExactRounding(final BigDecimal value, final int extraPrecision) {
    final String significand = value.unscaledValue().abs().toString();
    final int undiscardedPrecision = Math.max(0, significand.length() - extraPrecision);

    for (int i = undiscardedPrecision; i < significand.length(); ++i) {
      if (significand.charAt(i) != '0') {
        throw new NumberFormatException(
            "Conversion to Decimal128 would require inexact rounding of " + value);
      }
    }
    return undiscardedPrecision;
  }
}
