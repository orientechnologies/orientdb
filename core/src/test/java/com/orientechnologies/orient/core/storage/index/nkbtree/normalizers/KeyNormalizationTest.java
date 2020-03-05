package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.ibm.icu.text.Collator;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
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
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(null);
    Assert.assertEquals(1, compositeKey.getKeys().size());

    final OType[] types = new OType[1];
    types[0] = null;

    final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    print(bytes);
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
    Assert.assertEquals((new byte[]{(byte) 0x5})[0], bytes[2]);
  }

  @Test
  public void normalizeComposite_float() throws Exception {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(1.5f);
    Assert.assertEquals(1, compositeKey.getKeys().size());

    final OType[] types = new OType[1];
    types[0] = OType.FLOAT;

    final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    print(bytes);
    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[]{(byte) 0x3f})[0], bytes[4]);
  }

  @Test
  public void normalizeComposite_double() throws Exception {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(1.5d);
    Assert.assertEquals(1, compositeKey.getKeys().size());

    final OType[] types = new OType[1];
    types[0] = OType.DOUBLE;

    final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    print(bytes);
    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[]{(byte) 0xf8})[0], bytes[7]);
    Assert.assertEquals((new byte[]{(byte) 0x3f})[0], bytes[8]);
  }

  @Test
  public void normalizeComposite_bigDecimal() throws Exception {
    final OCompositeKey compositeKey = new OCompositeKey();
    final BigDecimal bigDecimal = new BigDecimal(3.14159265359);
    bigDecimal.setScale(2, RoundingMode.UP);
    compositeKey.addKey(bigDecimal);
    Assert.assertEquals(1, compositeKey.getKeys().size());

    final OType[] types = new OType[1];
    types[0] = OType.DECIMAL;

    final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    print(bytes);
    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    // Assert.assertEquals((new byte[]{(byte) 0x32})[0], bytes[1]);
    // Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[2]);
    // Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[3]);
    // Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[4]);
    //Assert.assertEquals((new byte[]{(byte) 0xad})[0], bytes[1]);
    //Assert.assertEquals((new byte[]{(byte) 0xf8})[0], bytes[2]);
    //Assert.assertEquals((new byte[]{(byte) 0xa1})[0], bytes[3]);
    //Assert.assertEquals((new byte[]{(byte) 0xa0})[0], bytes[4]);
    //Assert.assertEquals((new byte[]{(byte) 0x87})[0], bytes[5]);
    //Assert.assertEquals((new byte[]{(byte) 0x85})[0], bytes[6]);
    //Assert.assertEquals((new byte[]{(byte) 0x91})[0], bytes[7]);
    //Assert.assertEquals((new byte[]{(byte) 0x85})[0], bytes[8]);
  }

  @Test
  public void normalizeComposite_Boolean() throws Exception {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(true);
    Assert.assertEquals(1, compositeKey.getKeys().size());

    final OType[] types = new OType[1];
    types[0] = OType.BOOLEAN;

    final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    print(bytes);
    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[]{(byte) 0x1})[0], bytes[1]);
  }

  @Test
  public void normalizeComposite_long() throws Exception {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(5L);
    Assert.assertEquals(1, compositeKey.getKeys().size());

    final OType[] types = new OType[1];
    types[0] = OType.LONG;

    final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    print(bytes);
    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[]{(byte) 0x5})[0], bytes[1]);
  }

  @Test
  public void normalizeComposite_byte() throws Exception {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey((byte) 3);
    Assert.assertEquals(1, compositeKey.getKeys().size());

    final OType[] types = new OType[1];
    types[0] = OType.BYTE;

    final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    print(bytes);
    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[]{(byte) 0x3})[0], bytes[1]);
  }

  @Test
  public void normalizeComposite_short() throws Exception {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey((short) 3);
    Assert.assertEquals(1, compositeKey.getKeys().size());

    final OType[] types = new OType[1];
    types[0] = OType.SHORT;

    final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    print(bytes);
    Assert.assertEquals((new byte[]{(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[]{(byte) 0x3})[0], bytes[1]);
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

/*@Test
public void normalizeComposite_char() throws Exception {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey('a');
    Assert.assertEquals(1, compositeKey.getKeys().size());

    final OType[] types = new OType[1];
    types[0] = OType.;

    final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    Assert.assertEquals((new byte[] {(byte) 0x0})[0], bytes[0]);
    Assert.assertEquals((new byte[] {(byte) 0x61})[0], bytes[1]);
}*/

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
