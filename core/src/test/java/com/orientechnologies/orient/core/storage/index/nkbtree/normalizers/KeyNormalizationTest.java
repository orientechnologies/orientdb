package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.ibm.icu.text.Collator;
import com.orientechnologies.common.comparator.OByteArrayComparator;
import com.orientechnologies.common.comparator.OUnsafeByteArrayComparator;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.function.Consumer;

public class KeyNormalizationTest {
  KeyNormalizer keyNormalizer;

  @Before
  public void setup() {
    keyNormalizer = new KeyNormalizer();
  }

  @Test(expected = IllegalArgumentException.class)
  public void normalize_nullInput() throws Exception {
    keyNormalizer.normalize(null, null, Collator.NO_DECOMPOSITION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void normalize_unequalInput() throws Exception {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(null);
    keyNormalizer.normalize(compositeKey, new OType[0], Collator.NO_DECOMPOSITION);
  }

  @Test
  public void normalizeComposite_null() throws Exception {
    final byte[] bytes = getNormalizedKeySingle(null, null);

    Assert.assertEquals((new byte[]{(byte) 0x1})[0], bytes[0]);
  }

  @Test
  public void normalizeComposite_null_int() throws Exception {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(null);
    compositeKey.addKey(5);
    Assert.assertEquals(2, compositeKey.getKeys().size());

    final OType[] types = new OType[2];
    types[0] = null;
    types[1] = OType.INTEGER;

    final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    print(bytes);
    Assert.assertEquals((new byte[]{(byte) 0x1})[0], bytes[0]);
    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[1]);
    Assert.assertEquals((new byte[]{(byte) 0x80})[0], bytes[2]);
    Assert.assertEquals((new byte[]{(byte) 0x5})[0], bytes[5]);
  }

  @Test
  public void normalizeComposite_int() throws Exception {
    final byte[] bytes = getNormalizedKeySingle(5, OType.INTEGER);

    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[]{(byte) 0x5})[0], bytes[4]);
  }

  @Test
  public void normalizeComposite_intZero() throws Exception {
    final byte[] bytes = getNormalizedKeySingle(0, OType.INTEGER);

    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[4]);
  }

  @Test
  public void normalizeComposite_negInt() throws Exception {
    final byte[] bytes = getNormalizedKeySingle(-62, OType.INTEGER);

    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    // -62 signed := 4294967234 unsigned := FFFFFFC2 hex
    Assert.assertEquals((new byte[]{(byte) 0x7f})[0], bytes[1]);
    Assert.assertEquals((new byte[]{(byte) 0xff})[0], bytes[2]);
    Assert.assertEquals((new byte[]{(byte) 0xff})[0], bytes[3]);
    Assert.assertEquals((new byte[]{(byte) 0xc2})[0], bytes[4]);
  }

  @Test
  public void normalizeComposite_int_compare() throws Exception {
    final byte[] negative = getNormalizedKeySingle(-62, OType.INTEGER);
    final byte[] zero     = getNormalizedKeySingle(0, OType.INTEGER);
    final byte[] positive = getNormalizedKeySingle(5, OType.INTEGER);

    final OUnsafeByteArrayComparator byteArrayComparator = new OUnsafeByteArrayComparator();
    // 0x0 0x7f 0xff 0xff 0xc2 < 0x0 0x80 0x0 0x0 0x0
    Assert.assertTrue(0 > byteArrayComparator.compare(negative, zero));
    // 0x0 0x80 0x0 0x0 0x5 > 0x0 0x80 0x0 0x0 0x0
    Assert.assertTrue(0 < byteArrayComparator.compare(positive, zero));
    // 0x0 0x80 0x0 0x0 0x0 == 0x0 0x80 0x0 0x0 0x0
    Assert.assertTrue(0 == byteArrayComparator.compare(zero, zero));

    final OByteArrayComparator arrayComparator = new OByteArrayComparator();
    Assert.assertTrue(0 > arrayComparator.compare(negative, zero));
    Assert.assertTrue(0 < arrayComparator.compare(positive, zero));
    Assert.assertTrue(0 == arrayComparator.compare(zero, zero));
  }

  @Test
  public void normalizeComposite_float() throws Exception {
    final byte[] bytes = getNormalizedKeySingle(1.5f, OType.FLOAT);

    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[]{(byte) 0x3f})[0], bytes[1]);
    Assert.assertEquals((new byte[]{(byte) 0xc0})[0], bytes[2]);
  }

  @Test
  public void normalizeComposite_double() throws Exception {
    final byte[] bytes = getNormalizedKeySingle(1.5d, OType.DOUBLE);

    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[]{(byte) 0x3f})[0], bytes[1]);
    Assert.assertEquals((new byte[]{(byte) 0xf8})[0], bytes[2]);
  }

  @Test
  public void normalizeComposite_bigDecimal() throws Exception {
    final OCompositeKey compositeKey = new OCompositeKey();

    compositeKey.addKey(new BigDecimal(3.14159265359));
    Assert.assertEquals(1, compositeKey.getKeys().size());

    final OType[] types = new OType[1];
    types[0] = OType.DECIMAL;

    final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    print(bytes);
    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
  }

  @Test
  public void normalizeComposite_decimal() throws Exception {
    final OCompositeKey compositeKey = new OCompositeKey();

    compositeKey.addKey(new BigDecimal(3.14159265359));
    Assert.assertEquals(1, compositeKey.getKeys().size());

    final OType[] types = new OType[1];
    types[0] = OType.DECIMAL;

    final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    print(bytes);
    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
  }

  @Test
  public void normalizeComposite_Boolean() throws Exception {
    final byte[] bytes = getNormalizedKeySingle(true, OType.BOOLEAN);

    Assert.assertEquals((new byte[]{(byte) 0x1})[0], bytes[0]);
  }

  @Test
  public void normalizeComposite_long() throws Exception {
    final byte[] bytes = getNormalizedKeySingle(5L, OType.LONG);

    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[]{(byte) 0x80})[0], bytes[1]);
    Assert.assertEquals((new byte[]{(byte) 0x5})[0], bytes[8]);
  }

  @Test
  public void normalizeComposite_negLong() throws Exception {
    final byte[] bytes = getNormalizedKeySingle(-62L, OType.LONG);

    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[]{(byte) 0x7f})[0], bytes[1]);
    Assert.assertEquals((new byte[]{(byte) 0xff})[0], bytes[2]);
    Assert.assertEquals((new byte[]{(byte) 0xff})[0], bytes[3]);
    Assert.assertEquals((new byte[]{(byte) 0xff})[0], bytes[4]);
    Assert.assertEquals((new byte[]{(byte) 0xff})[0], bytes[5]);
    Assert.assertEquals((new byte[]{(byte) 0xff})[0], bytes[6]);
    Assert.assertEquals((new byte[]{(byte) 0xff})[0], bytes[7]);
    Assert.assertEquals((new byte[]{(byte) 0xc2})[0], bytes[8]);
  }

  @Test
  public void normalizeComposite_long_compare() throws Exception {
    final byte[] negative = getNormalizedKeySingle(-62L, OType.LONG);
    final byte[] zero     = getNormalizedKeySingle(0L, OType.LONG);
    final byte[] positive = getNormalizedKeySingle(5L, OType.LONG);

    final OUnsafeByteArrayComparator byteArrayComparator = new OUnsafeByteArrayComparator();
    Assert.assertTrue(0 > byteArrayComparator.compare(negative, zero));
    Assert.assertTrue(0 < byteArrayComparator.compare(positive, zero));
    Assert.assertTrue(0 == byteArrayComparator.compare(zero, zero));

    final OByteArrayComparator arrayComparator = new OByteArrayComparator();
    Assert.assertTrue(0 > arrayComparator.compare(negative, zero));
    Assert.assertTrue(0 < arrayComparator.compare(positive, zero));
    Assert.assertTrue(0 == arrayComparator.compare(zero, zero));
  }

  private byte getMostSignificantBit(final byte aByte) {
    return (byte) ((aByte & 0xFF00) >> 8);
  }

  private byte getLeastSignificantBit(final byte aByte) {
    return (byte) ((aByte & 0xFF) >> 8);
  }

  @Test
  public void normalizeComposite_byte() throws Exception {
    final byte[] bytes = getNormalizedKeySingle((byte) 3, OType.BYTE);

    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[]{(byte) 0x83})[0], bytes[1]);
  }

  @Test
  public void normalizeComposite_negByte() throws Exception {
    final byte[] bytes = getNormalizedKeySingle((byte) -62, OType.BYTE);

    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[]{(byte) 0x42})[0], bytes[1]);
  }

  @Test
  public void normalizeComposite_byte_compare() throws Exception {
    final byte[] negative = getNormalizedKeySingle((byte) -62, OType.BYTE);
    print(negative);
    final byte[] zero     = getNormalizedKeySingle((byte) 0, OType.BYTE);
    print(zero);
    final byte[] positive = getNormalizedKeySingle((byte) 5, OType.BYTE);
    print(positive);

    final OUnsafeByteArrayComparator byteArrayComparator = new OUnsafeByteArrayComparator();
    Assert.assertTrue(0 > byteArrayComparator.compare(negative, zero));
    Assert.assertTrue(0 < byteArrayComparator.compare(positive, zero));
    Assert.assertTrue(0 == byteArrayComparator.compare(zero, zero));

    final OByteArrayComparator arrayComparator = new OByteArrayComparator();
    Assert.assertTrue(0 > arrayComparator.compare(negative, zero));
    Assert.assertTrue(0 < arrayComparator.compare(positive, zero));
    Assert.assertTrue(0 == arrayComparator.compare(zero, zero));
  }

  @Test
  public void normalizeComposite_short() throws Exception {
    final byte[] bytes = getNormalizedKeySingle((short) 3, OType.SHORT);

    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[]{(byte) 0x80})[0], bytes[1]);
    Assert.assertEquals((new byte[]{(byte) 0x3})[0], bytes[2]);
  }

  @Test
  public void normalizeComposite_negShort() throws Exception {
    final byte[] bytes = getNormalizedKeySingle((short) -62, OType.SHORT);

    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[]{(byte) 0x7f})[0], bytes[1]);
    Assert.assertEquals((new byte[]{(byte) 0xc2})[0], bytes[2]);
  }

  @Test
  public void normalizeComposite_short_compare() throws Exception {
    final byte[] negative = getNormalizedKeySingle((short) -62, OType.SHORT);
    print(negative);
    final byte[] zero     = getNormalizedKeySingle((short) 0, OType.SHORT);
    print(zero);
    final byte[] positive = getNormalizedKeySingle((short) 5, OType.SHORT);
    print(positive);

    final OUnsafeByteArrayComparator byteArrayComparator = new OUnsafeByteArrayComparator();
    Assert.assertTrue(0 > byteArrayComparator.compare(negative, zero));
    Assert.assertTrue(0 < byteArrayComparator.compare(positive, zero));
    Assert.assertTrue(0 == byteArrayComparator.compare(zero, zero));

    final OByteArrayComparator arrayComparator = new OByteArrayComparator();
    Assert.assertTrue(0 > arrayComparator.compare(negative, zero));
    Assert.assertTrue(0 < arrayComparator.compare(positive, zero));
    Assert.assertTrue(0 == arrayComparator.compare(zero, zero));
  }

  @Test
  public void normalizeComposite_string() throws Exception {
    final OType[] types = new OType[1];
    types[0] = OType.STRING;

    assertCollationOfCompositeKeyString(types, getCompositeKey("abc"), (byte[] bytes) -> {
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

    assertCollationOfCompositeKeyString(types, getCompositeKey("Abc"), (byte[] bytes) -> {
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

    assertCollationOfCompositeKeyString(types, getCompositeKey("abC"), (byte[] bytes) -> {
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
  public void normalizeComposite_stringUmlaute() throws Exception {
    final OType[] types = new OType[1];
    types[0] = OType.STRING;

    assertCollationOfCompositeKeyString(types, getCompositeKey("ü"), (byte[] bytes) -> {
      Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
      Assert.assertEquals((new byte[] {(byte) 0x52})[0], bytes[1]);
      Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[2]);
      Assert.assertEquals((new byte[] {(byte) 0x45})[0], bytes[3]);
      Assert.assertEquals((new byte[] {(byte) 0x96})[0], bytes[4]);
      Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[5]);
      Assert.assertEquals((new byte[] {(byte) 0x6})[0], bytes[6]);
      Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[7]);
    });

    assertCollationOfCompositeKeyString(types, getCompositeKey("u"), (byte[] bytes) -> {
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
  public void normalizeComposite_two_strings() throws Exception {
    final OCompositeKey compositeKey = new OCompositeKey();
    final String key = "abcd";
    compositeKey.addKey(key);
    final String secondKey = "test";
    compositeKey.addKey(secondKey);
    Assert.assertEquals(2, compositeKey.getKeys().size());

    final OType[] types = new OType[2];
    types[0] = OType.STRING;
    types[1] = OType.STRING;

    assertCollationOfCompositeKeyString(types, compositeKey, (byte[] bytes) -> {
      // check 'not null' and beginning of first entry
      Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
      Assert.assertEquals((new byte[] {(byte) 0x2a})[0], bytes[1]);

      // finally assert 'not null' for second entry ..
      Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[10]);
      Assert.assertEquals((new byte[] {(byte) 0x50})[0], bytes[11]);
    });
  }

  @Test
  public void normalizeComposite_date() {
    final Date key = new GregorianCalendar(2013, Calendar.NOVEMBER, 5).getTime();
    final byte[] bytes = getNormalizedKeySingle(key, OType.DATE);

    // 1383606000000 := Tue Nov 05 2013 00:00:00
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[1]);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[2]);
    Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[3]);
    Assert.assertEquals((new byte[] {(byte) 0x42})[0], bytes[4]);
    Assert.assertEquals((new byte[] {(byte) 0x25})[0], bytes[5]);
    Assert.assertEquals((new byte[] {(byte) 0x58})[0], bytes[6]);
    Assert.assertEquals((new byte[] {(byte) 0x19})[0], bytes[7]);
    Assert.assertEquals((new byte[] {(byte) 0x80})[0], bytes[8]);
  }

  @Test
  public void normalizeComposite_dateTime() {
    final LocalDateTime ldt = LocalDateTime.of(2013, 11, 5, 3, 3, 3);
    final Date key = Date.from( ldt.atZone( ZoneId.systemDefault()).toInstant());
    final byte[] bytes = getNormalizedKeySingle(key, OType.DATETIME);

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
  public void normalizeComposite_binary() {
    final byte[] key = new byte[] { 1, 2, 3, 4, 5, 6 };

    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(key);
    Assert.assertEquals(1, compositeKey.getKeys().size());

    final OType[] types = new OType[1];
    types[0] = OType.BINARY;

    final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    print(bytes);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0x1})[0], bytes[1]);
    Assert.assertEquals((new byte[] {(byte) 0x6})[0], bytes[6]);
  }

  private byte[] getNormalizedKeySingle(final Object keyValue, final OType type) {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(keyValue);
    Assert.assertEquals(1, compositeKey.getKeys().size());

    final OType[] types = new OType[1];
    types[0] = type;

    return keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  private void assertCollationOfCompositeKeyString(final OType[] types, final OCompositeKey compositeKey, final Consumer<byte[]> func) {
    System.out.println("actual string: " + compositeKey.getKeys().get(0));
    final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    print(bytes);
    func.accept(bytes);
  }

  private OCompositeKey getCompositeKey(final String text) {
    final OCompositeKey compositeKey = new OCompositeKey();
    final String key = text;
    compositeKey.addKey(key);
    Assert.assertEquals(1, compositeKey.getKeys().size());
    return compositeKey;
  }

  private void print(final byte[] bytes) {
    for (final byte b : bytes) {
      System.out.format("0x%x ", b);
    }
    System.out.println();
  }
}
