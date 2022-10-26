package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.orientechnologies.common.comparator.OByteArrayComparator;
import com.orientechnologies.common.comparator.OUnsafeByteArrayComparator;
import com.orientechnologies.common.comparator.OUnsafeByteArrayComparatorV2;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.Collator;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.function.Consumer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class KeyNormalizationTest {
  KeyNormalizer keyNormalizer;

  @Before
  public void setup() {
    keyNormalizer = new KeyNormalizer();
  }

  @Test(expected = IllegalArgumentException.class)
  public void normalizeNullInput() {
    keyNormalizer.normalize(null, null, Collator.NO_DECOMPOSITION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void normalizeUnequalInput() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(null);
    keyNormalizer.normalize(compositeKey, new OType[0], Collator.NO_DECOMPOSITION);
  }

  @Test
  public void normalizeCompositeNull() {
    final byte[] bytes = getNormalizedKeySingle(null, null);
    Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[0]);
  }

  @Test
  public void normalizeCompositeNullInt() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(null);
    compositeKey.addKey(5);
    Assert.assertEquals(2, compositeKey.getKeys().size());

    final OType[] types = new OType[2];
    types[0] = null;
    types[1] = OType.INTEGER;

    final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[1]);
    Assert.assertEquals((new byte[] {(byte) 0x80})[0], bytes[2]);
    Assert.assertEquals((new byte[] {(byte) 0x5})[0], bytes[5]);
  }

  @Test
  public void normalizeCompositeInt() {
    final byte[] bytes = getNormalizedKeySingle(5, OType.INTEGER);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0x5})[0], bytes[4]);
  }

  @Test
  public void normalizeCompositeIntZero() {
    final byte[] bytes = getNormalizedKeySingle(0, OType.INTEGER);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[4]);
  }

  @Test
  public void normalizeCompositeNegInt() {
    final byte[] bytes = getNormalizedKeySingle(-62, OType.INTEGER);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    // -62 signed := 4294967234 unsigned := FFFFFFC2 hex
    Assert.assertEquals((new byte[] {(byte) 0x7f})[0], bytes[1]);
    Assert.assertEquals((new byte[] {(byte) 0xff})[0], bytes[2]);
    Assert.assertEquals((new byte[] {(byte) 0xff})[0], bytes[3]);
    Assert.assertEquals((new byte[] {(byte) 0xc2})[0], bytes[4]);
  }

  @Test
  public void normalizeCompositeIntCompare() {
    final byte[] negative = getNormalizedKeySingle(-62, OType.INTEGER);
    final byte[] zero = getNormalizedKeySingle(0, OType.INTEGER);
    final byte[] positive = getNormalizedKeySingle(5, OType.INTEGER);
    compareWithUnsafeByteArrayComparator(negative, zero, positive);
    compareWithByteArrayComparator(negative, zero, positive);
  }

  // long running, move to separate test?
  @Ignore
  @Test
  public void normalizeCompositeIntCompareLoop() {
    final byte[] zero = getNormalizedKeySingle(0, OType.INTEGER);

    for (int i = 1; i < Integer.MAX_VALUE; ++i) {
      final byte[] negative = getNormalizedKeySingle(-1 * i, OType.INTEGER);
      final byte[] positive = getNormalizedKeySingle(i, OType.INTEGER);
      compareWithUnsafeByteArrayComparator(negative, zero, positive);
      compareWithByteArrayComparator(negative, zero, positive);
    }
  }

  @Test
  public void normalizeCompositeDouble() {
    final byte[] bytes = getNormalizedKeySingle(1.5d, OType.DOUBLE);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0xbf})[0], bytes[1]);
    Assert.assertEquals((new byte[] {(byte) 0xf8})[0], bytes[2]);
  }

  @Test
  public void normalizeCompositeDoubleCompare() {
    final byte[] negative = getNormalizedKeySingle(-62.0d, OType.DOUBLE);
    final byte[] zero = getNormalizedKeySingle(0.0d, OType.DOUBLE);
    final byte[] positive = getNormalizedKeySingle(1.5d, OType.DOUBLE);
    compareWithUnsafeByteArrayComparator(negative, zero, positive);
    compareWithByteArrayComparator(negative, zero, positive);
  }

  @Test
  public void normalizeCompositeFloat() {
    final byte[] bytes = getNormalizedKeySingle(1.5f, OType.FLOAT);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0xbf})[0], bytes[1]);
    Assert.assertEquals((new byte[] {(byte) 0xc0})[0], bytes[2]);
  }

  @Test
  public void normalizeCompositeFloatCompare() {
    final byte[] negative = getNormalizedKeySingle(-62.0f, OType.FLOAT);
    final byte[] zero = getNormalizedKeySingle(0.0f, OType.FLOAT);
    final byte[] positive = getNormalizedKeySingle(1.5f, OType.FLOAT);
    compareWithUnsafeByteArrayComparator(negative, zero, positive);
    compareWithByteArrayComparator(negative, zero, positive);
  }

  // we do not compare across data types
  @Ignore
  @Test
  public void normalizeCompositeDoubleFloatCompare() {
    final byte[] negative = getNormalizedKeySingle(-62.5, OType.DOUBLE);
    final byte[] zero = getNormalizedKeySingle(-61.75f, OType.FLOAT);
    final byte[] smallerNegative = getNormalizedKeySingle(-61.0d, OType.DOUBLE);
    final byte[] smallerNegativeFloat = getNormalizedKeySingle(-61.0f, OType.FLOAT);
    compareWithUnsafeByteArrayComparatorIntertype(smallerNegative, smallerNegativeFloat);
    compareWithUnsafeByteArrayComparator(negative, zero, smallerNegative);
    compareWithByteArrayComparator(negative, zero, smallerNegative);
  }

  // we do not compare across data types
  @Ignore
  @Test
  public void normalizeCompositeIntFloatCompare() {
    final byte[] negative = getNormalizedKeySingle(-62, OType.INTEGER);
    final byte[] zero = getNormalizedKeySingle(-61.75f, OType.FLOAT);
    final byte[] smallerNegative = getNormalizedKeySingle(-61, OType.INTEGER);
    final byte[] smallerNegativeFloat = getNormalizedKeySingle(-61f, OType.FLOAT);
    compareWithUnsafeByteArrayComparatorIntertype(smallerNegative, smallerNegativeFloat);
    compareWithUnsafeByteArrayComparator(negative, zero, smallerNegative);
    compareWithByteArrayComparator(negative, zero, smallerNegative);
  }

  @Test
  public void testBigDecimal() {
    final BigDecimal matKey = new BigDecimal(87866787879879879768767554645.434);
    final ByteBuffer bb = ByteBuffer.allocate(1 + 8); // bytes.length);
    bb.order(ByteOrder.BIG_ENDIAN);
    bb.put((byte) 0);
    bb.putLong(Double.doubleToLongBits(matKey.doubleValue()) + Long.MAX_VALUE + 1);
    print(bb.array());

    System.out.println(matKey.toPlainString() + "-" + matKey.toEngineeringString());
    byte[] bytes = getNormalizedKeySingle(matKey.toPlainString(), OType.STRING);
    print(bytes);
    byte[] bytesNeg =
        getNormalizedKeySingle(new BigDecimal(-3.14159265359).toPlainString(), OType.STRING);
    print(bytesNeg);
    byte[] bytesMoreNeg =
        getNormalizedKeySingle(new BigDecimal(-3.14159265359).toPlainString(), OType.STRING);
    print(bytesMoreNeg);

    // compareWithUnsafeByteArrayComparator(bytesMoreNeg, bytesNeg, bytes);

    /*
     * final byte[] stream = new byte[9]; ODecimalSerializer decimalSerializer = new ODecimalSerializer();
     * decimalSerializer.serialize(matKey, stream, 0); print(ByteBuffer.wrap(stream).order(ByteOrder.BIG_ENDIAN).array());
     * Assert.assertEquals(decimalSerializer.deserialize(stream, 0), matKey);
     *
     * final BigDecimal matKeyNeg = new BigDecimal(new BigInteger("-2"), 2); final byte[] streamNeg = new byte[9];
     * decimalSerializer.serialize(matKeyNeg, streamNeg, 0); print(ByteBuffer.wrap(streamNeg).order(ByteOrder.BIG_ENDIAN).array());
     * Assert.assertEquals(decimalSerializer.deserialize(streamNeg, 0), matKeyNeg);
     *
     * final BigDecimal matKeyZero = new BigDecimal(new BigInteger("0"), 2); final byte[] streamZero = new byte[9];
     * decimalSerializer.serialize(matKeyZero, streamZero, 0);
     * print(ByteBuffer.wrap(streamZero).order(ByteOrder.BIG_ENDIAN).array());
     * Assert.assertEquals(decimalSerializer.deserialize(streamZero, 0), matKeyZero);
     *
     * compareWithUnsafeByteArrayComparator(streamNeg, streamZero, stream);
     */
  }

  @Test
  public void normalizeCompositeBigDecimal() {
    final byte[] bytes = getNormalizedKeySingle(new BigDecimal("3.14159265359"), OType.DECIMAL);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0xc0})[0], bytes[1]);
    Assert.assertEquals((new byte[] {(byte) 0x9})[0], bytes[2]);
    Assert.assertEquals((new byte[] {(byte) 0x21})[0], bytes[3]);
    Assert.assertEquals((new byte[] {(byte) 0xfb})[0], bytes[4]);
    Assert.assertEquals((new byte[] {(byte) 0x54})[0], bytes[5]);
    Assert.assertEquals((new byte[] {(byte) 0x44})[0], bytes[6]);
    Assert.assertEquals((new byte[] {(byte) 0x2e})[0], bytes[7]);
    Assert.assertEquals((new byte[] {(byte) 0xea})[0], bytes[8]);
  }

  @Test
  public void normalizeCompositeNegBigDecimal() {
    final byte[] bytes = getNormalizedKeySingle(new BigDecimal("-3.14159265359"), OType.DECIMAL);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0x40})[0], bytes[1]);
    Assert.assertEquals((new byte[] {(byte) 0x9})[0], bytes[2]);
    Assert.assertEquals((new byte[] {(byte) 0x21})[0], bytes[3]);
    Assert.assertEquals((new byte[] {(byte) 0xfb})[0], bytes[4]);
    Assert.assertEquals((new byte[] {(byte) 0x54})[0], bytes[5]);
    Assert.assertEquals((new byte[] {(byte) 0x44})[0], bytes[6]);
    Assert.assertEquals((new byte[] {(byte) 0x2e})[0], bytes[7]);
    Assert.assertEquals((new byte[] {(byte) 0xea})[0], bytes[8]);
  }

  @Test
  public void normalizeCompositeDecimalCompare() {
    final byte[] negative = getNormalizedKeySingle(new BigDecimal("-3.14159265359"), OType.DECIMAL);
    final byte[] zero = getNormalizedKeySingle(new BigDecimal("0.0"), OType.DECIMAL);
    final byte[] zero2 =
        getNormalizedKeySingle(new BigDecimal(new BigInteger("0"), 2), OType.DECIMAL);
    final byte[] positive = getNormalizedKeySingle(new BigDecimal(3.14159265359), OType.DECIMAL);
    final byte[] positive2 =
        getNormalizedKeySingle(new BigDecimal(new BigInteger("314159265359"), 11), OType.DECIMAL);
    compareWithUnsafeByteArrayComparator(negative, zero, positive);
    compareWithByteArrayComparator(negative, zero, positive);
    compareWithUnsafeByteArrayComparatorIntertype(zero, zero2);
    compareWithUnsafeByteArrayComparatorIntertype(positive, positive2);
  }

  @Test
  public void normalizeCompositeBoolean() {
    final byte[] bytes = getNormalizedKeySingle(true, OType.BOOLEAN);
    Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[0]);
  }

  @Test
  public void normalizeCompositeLong() {
    final byte[] bytes = getNormalizedKeySingle(5L, OType.LONG);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0x80})[0], bytes[1]);
    Assert.assertEquals((new byte[] {(byte) 0x5})[0], bytes[8]);
  }

  @Test
  public void normalizeCompositeNegLong() {
    final byte[] bytes = getNormalizedKeySingle(-62L, OType.LONG);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0x7f})[0], bytes[1]);
    Assert.assertEquals((new byte[] {(byte) 0xff})[0], bytes[2]);
    Assert.assertEquals((new byte[] {(byte) 0xff})[0], bytes[3]);
    Assert.assertEquals((new byte[] {(byte) 0xff})[0], bytes[4]);
    Assert.assertEquals((new byte[] {(byte) 0xff})[0], bytes[5]);
    Assert.assertEquals((new byte[] {(byte) 0xff})[0], bytes[6]);
    Assert.assertEquals((new byte[] {(byte) 0xff})[0], bytes[7]);
    Assert.assertEquals((new byte[] {(byte) 0xc2})[0], bytes[8]);
  }

  @Test
  public void normalizeCompositeLongCompare() {
    final byte[] negative = getNormalizedKeySingle(-62L, OType.LONG);
    final byte[] zero = getNormalizedKeySingle(0L, OType.LONG);
    final byte[] positive = getNormalizedKeySingle(5L, OType.LONG);
    compareWithUnsafeByteArrayComparator(negative, zero, positive);
    compareWithByteArrayComparator(negative, zero, positive);
  }

  private byte getMostSignificantBit(final byte aByte) {
    return (byte) ((aByte & 0xFF00) >> 8);
  }

  private byte getLeastSignificantBit(final byte aByte) {
    return (byte) ((aByte & 0xFF) >> 8);
  }

  @Test
  public void normalizeCompositeByte() {
    final byte[] bytes = getNormalizedKeySingle((byte) 3, OType.BYTE);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0x83})[0], bytes[1]);
  }

  @Test
  public void normalizeCompositeNegByte() {
    final byte[] bytes = getNormalizedKeySingle((byte) -62, OType.BYTE);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0x42})[0], bytes[1]);
  }

  @Test
  public void normalizeCompositeByteCompare() {
    final byte[] negative = getNormalizedKeySingle((byte) -62, OType.BYTE);
    final byte[] zero = getNormalizedKeySingle((byte) 0, OType.BYTE);
    final byte[] positive = getNormalizedKeySingle((byte) 5, OType.BYTE);
    compareWithUnsafeByteArrayComparator(negative, zero, positive);
    compareWithByteArrayComparator(negative, zero, positive);
  }

  @Test
  public void normalizeCompositeShort() {
    final byte[] bytes = getNormalizedKeySingle((short) 3, OType.SHORT);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0x80})[0], bytes[1]);
    Assert.assertEquals((new byte[] {(byte) 0x3})[0], bytes[2]);
  }

  @Test
  public void normalizeCompositeNegShort() {
    final byte[] bytes = getNormalizedKeySingle((short) -62, OType.SHORT);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0x7f})[0], bytes[1]);
    Assert.assertEquals((new byte[] {(byte) 0xc2})[0], bytes[2]);
  }

  @Test
  public void normalizeCompositeShortCompare() {
    final byte[] negative = getNormalizedKeySingle((short) -62, OType.SHORT);
    final byte[] zero = getNormalizedKeySingle((short) 0, OType.SHORT);
    final byte[] positive = getNormalizedKeySingle((short) 5, OType.SHORT);
    compareWithUnsafeByteArrayComparator(negative, zero, positive);
    compareWithByteArrayComparator(negative, zero, positive);
  }

  @Test
  @Ignore
  public void normalizeCompositeString() {
    final OType[] types = new OType[1];
    types[0] = OType.STRING;

    assertCollationOfCompositeKeyString(
        types,
        getCompositeKey("abc"),
        (byte[] bytes) -> {
          Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
          Assert.assertEquals((new byte[] {(byte) 0x2a})[0], bytes[1]);
          Assert.assertEquals((new byte[] {(byte) 0x2c})[0], bytes[2]);
          Assert.assertEquals((new byte[] {(byte) 0x2e})[0], bytes[3]);
          Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[4]);
          Assert.assertEquals((new byte[] {(byte) 0x7})[0], bytes[5]);
          Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[6]);
          Assert.assertEquals((new byte[] {(byte) 0x7})[0], bytes[7]);
          Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[8]);
        });

    assertCollationOfCompositeKeyString(
        types,
        getCompositeKey("Abc"),
        (byte[] bytes) -> {
          Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
          Assert.assertEquals((new byte[] {(byte) 0x2a})[0], bytes[1]);
          Assert.assertEquals((new byte[] {(byte) 0x2c})[0], bytes[2]);
          Assert.assertEquals((new byte[] {(byte) 0x2e})[0], bytes[3]);
          Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[4]);
          Assert.assertEquals((new byte[] {(byte) 0x7})[0], bytes[5]);
          Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[6]);
          Assert.assertEquals((new byte[] {(byte) 0xdc})[0], bytes[7]);
          Assert.assertEquals((new byte[] {(byte) 0x6})[0], bytes[8]);
          Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[9]);
        });

    assertCollationOfCompositeKeyString(
        types,
        getCompositeKey("abC"),
        (byte[] bytes) -> {
          Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
          Assert.assertEquals((new byte[] {(byte) 0x2a})[0], bytes[1]);
          Assert.assertEquals((new byte[] {(byte) 0x2c})[0], bytes[2]);
          Assert.assertEquals((new byte[] {(byte) 0x2e})[0], bytes[3]);
          Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[4]);
          Assert.assertEquals((new byte[] {(byte) 0x7})[0], bytes[5]);
          Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[6]);
          Assert.assertEquals((new byte[] {(byte) 0xc4})[0], bytes[7]);
          Assert.assertEquals((new byte[] {(byte) 0xdc})[0], bytes[8]);
          Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[9]);
        });
  }

  @Test
  public void normalizeCompositeStringCompare() {
    final byte[] smallest = getNormalizedKeySingle("a", OType.STRING);
    final byte[] middle = getNormalizedKeySingle("b", OType.STRING);
    final byte[] largest = getNormalizedKeySingle("c", OType.STRING);
    compareWithUnsafeByteArrayComparator(smallest, middle, largest);
    compareWithByteArrayComparator(smallest, middle, largest);
  }

  @Test
  @Ignore
  public void normalizeCompositeIntStringCompare() {
    final byte[] smallest = getNormalizedKeySingle("-1", OType.STRING);
    final byte[] smaller = getNormalizedKeySingle("-13", OType.STRING);
    final byte[] middle = getNormalizedKeySingle("0", OType.STRING);
    final byte[] largest = getNormalizedKeySingle("1", OType.STRING);
    compareWithUnsafeByteArrayComparator(smallest, middle, largest);
    compareWithUnsafeByteArrayComparator(smallest, smaller, middle);
  }

  @Test
  public void normalizeCompositeStringSequenceCompare() {
    // compare: http://demo.icu-project.org/icu-bin/collation.html
    final byte[] smallest = getNormalizedKeySingle("abc", OType.STRING);
    final byte[] middle = getNormalizedKeySingle("abC", OType.STRING);
    final byte[] largest = getNormalizedKeySingle("Abc", OType.STRING);
    compareWithUnsafeByteArrayComparator(smallest, middle, largest);
    compareWithByteArrayComparator(smallest, middle, largest);
  }

  @Test
  @Ignore
  public void normalizeCompositeStringUmlaute() {
    final OType[] types = new OType[1];
    types[0] = OType.STRING;

    assertCollationOfCompositeKeyString(
        types,
        getCompositeKey("ü"),
        (byte[] bytes) -> {
          Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
          Assert.assertEquals((new byte[] {(byte) 0x52})[0], bytes[1]);
          Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[2]);
          Assert.assertEquals((new byte[] {(byte) 0x45})[0], bytes[3]);
          Assert.assertEquals((new byte[] {(byte) 0x96})[0], bytes[4]);
          Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[5]);
          Assert.assertEquals((new byte[] {(byte) 0x6})[0], bytes[6]);
          Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[7]);
        });

    assertCollationOfCompositeKeyString(
        types,
        getCompositeKey("u"),
        (byte[] bytes) -> {
          Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
          Assert.assertEquals((new byte[] {(byte) 0x52})[0], bytes[1]);
          Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[2]);
          Assert.assertEquals((new byte[] {(byte) 0x5})[0], bytes[3]);
          Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[4]);
          Assert.assertEquals((new byte[] {(byte) 0x5})[0], bytes[5]);
          Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[6]);
        });
  }

  @Test
  @Ignore
  public void normalizeCompositeTwoStrings() {
    final OCompositeKey compositeKey = new OCompositeKey();
    final String key = "abcd";
    compositeKey.addKey(key);
    final String secondKey = "test";
    compositeKey.addKey(secondKey);
    Assert.assertEquals(2, compositeKey.getKeys().size());

    final OType[] types = new OType[2];
    types[0] = OType.STRING;
    types[1] = OType.STRING;

    assertCollationOfCompositeKeyString(
        types,
        compositeKey,
        (byte[] bytes) -> {
          // check 'not null' and beginning of first entry
          Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
          Assert.assertEquals((new byte[] {(byte) 0x2a})[0], bytes[1]);

          // finally assert 'not null' for second entry ..
          Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[10]);
          Assert.assertEquals((new byte[] {(byte) 0x50})[0], bytes[11]);
        });
  }

  @Test
  public void normalizeCompositeDate() {
    final GregorianCalendar calendar = getGregorianCalendarUTC(2013, Calendar.NOVEMBER, 5);
    final Date key = calendar.getTime();
    final byte[] bytes = getNormalizedKeySingle(key, OType.DATE);

    // 1383606000000 := Tue Nov 05 2013 00:00:00
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[1]);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[2]);
    Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[3]);
    Assert.assertEquals((new byte[] {(byte) 0x42})[0], bytes[4]);
    Assert.assertEquals((new byte[] {(byte) 0x25})[0], bytes[5]);
    Assert.assertEquals((new byte[] {(byte) 0x8f})[0], bytes[6]);
    Assert.assertEquals((new byte[] {(byte) 0x8})[0], bytes[7]);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[8]);
  }

  private GregorianCalendar getGregorianCalendarUTC(
      final int year, final int month, final int dayOfMonth) {
    final GregorianCalendar calendar = new GregorianCalendar(year, month, dayOfMonth);
    calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
    return calendar;
  }

  @Test
  public void normalizeCompositeDateCompare() {
    GregorianCalendar calendar = getGregorianCalendarUTC(2013, Calendar.AUGUST, 5);
    Date key = calendar.getTime();
    final byte[] smallest = getNormalizedKeySingle(key, OType.DATE);

    calendar = getGregorianCalendarUTC(2013, Calendar.NOVEMBER, 5);
    key = calendar.getTime();
    final byte[] middle = getNormalizedKeySingle(key, OType.DATETIME);

    calendar = getGregorianCalendarUTC(2013, Calendar.NOVEMBER, 6);
    key = calendar.getTime();
    final byte[] largest = getNormalizedKeySingle(key, OType.DATETIME);
    compareWithUnsafeByteArrayComparator(smallest, middle, largest);
    compareWithByteArrayComparator(smallest, middle, largest);
  }

  @Ignore
  @Test
  public void normalizeCompositeDateTime() {
    final ZonedDateTime zdt =
        LocalDateTime.of(2013, 11, 5, 3, 3, 3)
            .atZone(ZoneId.systemDefault())
            .withZoneSameInstant(ZoneId.of("UTC"));
    final Date key = Date.from(zdt.toInstant());
    System.out.println("Date-key: " + key);
    final byte[] bytes = getNormalizedKeySingle(key, OType.DATETIME);
    print(bytes);

    // 1383616983000 := Tue Nov 05 2013 03:03:03
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[1]);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[2]);
    Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[3]);
    Assert.assertEquals((new byte[] {(byte) 0x42})[0], bytes[4]);
    Assert.assertEquals((new byte[] {(byte) 0x25})[0], bytes[5]);
    Assert.assertEquals((new byte[] {(byte) 0xff})[0], bytes[6]);
    Assert.assertEquals((new byte[] {(byte) 0xaf})[0], bytes[7]);
    Assert.assertEquals((new byte[] {(byte) 0xd8})[0], bytes[8]);
  }

  @Test
  public void normalizeCompositeDateTimeCompare() {
    ZonedDateTime zdt =
        LocalDateTime.of(2013, 11, 5, 3, 3, 3)
            .atZone(ZoneId.systemDefault())
            .withZoneSameInstant(ZoneId.of("UTC"));
    Date key = Date.from(zdt.toInstant());
    final byte[] smallest = getNormalizedKeySingle(key, OType.DATETIME);

    zdt =
        LocalDateTime.of(2013, 11, 5, 5, 3, 3)
            .atZone(ZoneId.systemDefault())
            .withZoneSameInstant(ZoneId.of("UTC"));
    key = Date.from(zdt.toInstant());
    final byte[] middle = getNormalizedKeySingle(key, OType.DATETIME);

    zdt =
        LocalDateTime.of(2013, 11, 5, 5, 5, 5)
            .atZone(ZoneId.systemDefault())
            .withZoneSameInstant(ZoneId.of("UTC"));
    key = Date.from(zdt.toInstant());
    final byte[] largest = getNormalizedKeySingle(key, OType.DATETIME);
    compareWithUnsafeByteArrayComparator(smallest, middle, largest);
    compareWithByteArrayComparator(smallest, middle, largest);
  }

  @Test
  public void normalizeCompositeBinary() {
    final byte[] key = new byte[] {1, 2, 3, 4, 5, 6};

    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(key);
    Assert.assertEquals(1, compositeKey.getKeys().size());

    final OType[] types = new OType[1];
    types[0] = OType.BINARY;

    final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[1]);
    Assert.assertEquals((new byte[] {(byte) 0x6})[0], bytes[6]);
  }

  @Test
  public void normalizeCompositeBinaryCompare() {
    final byte[] smallest = getNormalizedKeySingle(new byte[] {1, 2, 3, 4, 5, 6}, OType.BINARY);
    final byte[] middle = getNormalizedKeySingle(new byte[] {2, 2, 3, 4, 5, 6}, OType.BINARY);
    final byte[] biggest = getNormalizedKeySingle(new byte[] {3, 2, 3, 4, 5, 6}, OType.BINARY);
    compareWithUnsafeByteArrayComparator(smallest, middle, biggest);
    compareWithByteArrayComparator(smallest, middle, biggest);
  }

  private byte[] getNormalizedKeySingle(final Object keyValue, final OType type) {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(keyValue);
    Assert.assertEquals(1, compositeKey.getKeys().size());

    final OType[] types = new OType[1];
    types[0] = type;
    return keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  private void assertCollationOfCompositeKeyString(
      final OType[] types, final OCompositeKey compositeKey, final Consumer<byte[]> func) {
    // System.out.println("actual string: " + compositeKey.getKeys().get(0));
    final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    // print(bytes);
    func.accept(bytes);
  }

  private OCompositeKey getCompositeKey(final String text) {
    final OCompositeKey compositeKey = new OCompositeKey();
    final String key = text;
    compositeKey.addKey(key);
    Assert.assertEquals(1, compositeKey.getKeys().size());
    return compositeKey;
  }

  private void compareWithByteArrayComparator(byte[] negative, byte[] zero, byte[] positive) {
    final OByteArrayComparator arrayComparator = new OByteArrayComparator();
    Assert.assertTrue("negative < zero", 0 > arrayComparator.compare(negative, zero));
    Assert.assertTrue("positive > zero", 0 < arrayComparator.compare(positive, zero));
    Assert.assertTrue("zero == zero", 0 == arrayComparator.compare(zero, zero));
    Assert.assertTrue("negative < positive", 0 > arrayComparator.compare(negative, positive));
  }

  private void compareWithUnsafeByteArrayComparator(byte[] negative, byte[] zero, byte[] positive) {
    final OUnsafeByteArrayComparatorV2 byteArrayComparator = new OUnsafeByteArrayComparatorV2();
    Assert.assertTrue("[unsafe] negative < zero", 0 > byteArrayComparator.compare(negative, zero));
    Assert.assertTrue("[unsafe] positive > zero", 0 < byteArrayComparator.compare(positive, zero));
    Assert.assertTrue("[unsafe] zero == zero", 0 == byteArrayComparator.compare(zero, zero));
    Assert.assertTrue(
        "{unsafe] negative < positive", 0 > byteArrayComparator.compare(negative, positive));
  }

  private void compareWithUnsafeByteArrayComparatorIntertype(byte[] first, byte[] second) {
    final OUnsafeByteArrayComparator byteArrayComparator = new OUnsafeByteArrayComparator();
    Assert.assertTrue("[unsafe] first == second", 0 == byteArrayComparator.compare(first, second));
  }

  private void print(final byte[] bytes) {
    for (final byte b : bytes) {
      System.out.format("0x%x ", b);
    }
    System.out.println();
  }
}
