package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.collate.OCaseInsensitiveCollate;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OBinaryField;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BinaryComparatorEqualsTest extends AbstractComparatorTest {

  @Before
  public void before() {
    ODatabaseRecordThreadLocal.instance().remove();
  }

  @Test
  public void testInteger() {
    testEquals(OType.INTEGER, 10);
  }

  @Test
  public void testLong() {
    testEquals(OType.LONG, 10l);
  }

  @Test
  public void testShort() {
    testEquals(OType.SHORT, (short) 10);
  }

  @Test
  public void testByte() {
    testEquals(OType.BYTE, (byte) 10);
  }

  @Test
  public void testFloat() {
    testEquals(OType.FLOAT, 10f);
  }

  @Test
  public void testDouble() {
    testEquals(OType.DOUBLE, 10d);
  }

  @Test
  public void testDatetime() throws ParseException {
    testEquals(OType.DATETIME, 10l);

    final SimpleDateFormat format =
        new SimpleDateFormat(OStorageConfiguration.DEFAULT_DATETIME_FORMAT);

    String now1 = format.format(new Date());
    Date now = format.parse(now1);

    Assert.assertTrue(
        comparator.isEqual(field(OType.DATETIME, now), field(OType.STRING, format.format(now))));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.DATETIME, new Date(now.getTime() + 1)),
            field(OType.STRING, format.format(now))));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.DATETIME, new Date(now.getTime() - 1)),
            field(OType.STRING, format.format(now))));
  }

  @Test
  public void testBinary() throws ParseException {
    final byte[] b1 = new byte[] {0, 1, 2, 3};
    final byte[] b2 = new byte[] {0, 1, 2, 4};
    final byte[] b3 = new byte[] {1, 1, 2, 4};

    Assert.assertTrue(comparator.isEqual(field(OType.BINARY, b1), field(OType.BINARY, b1)));
    Assert.assertFalse(comparator.isEqual(field(OType.BINARY, b1), field(OType.BINARY, b2)));
    Assert.assertFalse(comparator.isEqual(field(OType.BINARY, b1), field(OType.BINARY, b3)));
  }

  @Test
  public void testLinks() throws ParseException {
    Assert.assertTrue(
        comparator.isEqual(
            field(OType.LINK, new ORecordId(1, 2)), field(OType.LINK, new ORecordId(1, 2))));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.LINK, new ORecordId(1, 2)), field(OType.LINK, new ORecordId(2, 1))));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.LINK, new ORecordId(1, 2)), field(OType.LINK, new ORecordId(0, 2))));

    Assert.assertTrue(
        comparator.isEqual(
            field(OType.LINK, new ORecordId(1, 2)),
            field(OType.STRING, new ORecordId(1, 2).toString())));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.LINK, new ORecordId(1, 2)),
            field(OType.STRING, new ORecordId(0, 2).toString())));
  }

  @Test
  public void testString() {
    Assert.assertTrue(comparator.isEqual(field(OType.STRING, "test"), field(OType.STRING, "test")));
    Assert.assertFalse(
        comparator.isEqual(field(OType.STRING, "test2"), field(OType.STRING, "test")));
    Assert.assertFalse(
        comparator.isEqual(field(OType.STRING, "test"), field(OType.STRING, "test2")));
    Assert.assertFalse(comparator.isEqual(field(OType.STRING, "t"), field(OType.STRING, "te")));

    // DEF COLLATE
    Assert.assertTrue(
        comparator.isEqual(
            field(OType.STRING, "test", new ODefaultCollate()), field(OType.STRING, "test")));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.STRING, "test2", new ODefaultCollate()), field(OType.STRING, "test")));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.STRING, "test", new ODefaultCollate()), field(OType.STRING, "test2")));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.STRING, "t", new ODefaultCollate()), field(OType.STRING, "te")));

    Assert.assertTrue(
        comparator.isEqual(
            field(OType.STRING, "test", new ODefaultCollate()),
            field(OType.STRING, "test", new ODefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.STRING, "test2", new ODefaultCollate()),
            field(OType.STRING, "test", new ODefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.STRING, "test", new ODefaultCollate()),
            field(OType.STRING, "test2", new ODefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.STRING, "t", new ODefaultCollate()),
            field(OType.STRING, "te", new ODefaultCollate())));

    Assert.assertTrue(
        comparator.isEqual(
            field(OType.STRING, "test"), field(OType.STRING, "test", new ODefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.STRING, "test2"), field(OType.STRING, "test", new ODefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.STRING, "test"), field(OType.STRING, "test2", new ODefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.STRING, "t"), field(OType.STRING, "te", new ODefaultCollate())));

    // CASE INSENSITIVE COLLATE
    Assert.assertTrue(
        comparator.isEqual(
            field(OType.STRING, "test"),
            field(OType.STRING, "test", new OCaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.STRING, "test2"),
            field(OType.STRING, "test", new OCaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.STRING, "test"),
            field(OType.STRING, "test2", new OCaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.STRING, "t"), field(OType.STRING, "te", new OCaseInsensitiveCollate())));

    Assert.assertTrue(
        comparator.isEqual(
            field(OType.STRING, "test"),
            field(OType.STRING, "TEST", new OCaseInsensitiveCollate())));
    Assert.assertTrue(
        comparator.isEqual(
            field(OType.STRING, "TEST"),
            field(OType.STRING, "TEST", new OCaseInsensitiveCollate())));
    Assert.assertTrue(
        comparator.isEqual(
            field(OType.STRING, "TE"), field(OType.STRING, "te", new OCaseInsensitiveCollate())));

    Assert.assertFalse(
        comparator.isEqual(
            field(OType.STRING, "test2"),
            field(OType.STRING, "TEST", new OCaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.STRING, "test"),
            field(OType.STRING, "TEST2", new OCaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.STRING, "t"), field(OType.STRING, "tE", new OCaseInsensitiveCollate())));
  }

  @Test
  public void testDecimal() {
    Assert.assertTrue(
        comparator.isEqual(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.DECIMAL, new BigDecimal(10))));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.DECIMAL, new BigDecimal(11))));
    Assert.assertFalse(
        comparator.isEqual(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.DECIMAL, new BigDecimal(9))));
  }

  @Test
  public void testBoolean() {
    Assert.assertTrue(comparator.isEqual(field(OType.BOOLEAN, true), field(OType.BOOLEAN, true)));
    Assert.assertFalse(comparator.isEqual(field(OType.BOOLEAN, true), field(OType.BOOLEAN, false)));
    Assert.assertFalse(comparator.isEqual(field(OType.BOOLEAN, false), field(OType.BOOLEAN, true)));

    Assert.assertTrue(comparator.isEqual(field(OType.BOOLEAN, true), field(OType.STRING, "true")));
    Assert.assertTrue(
        comparator.isEqual(field(OType.BOOLEAN, false), field(OType.STRING, "false")));
    Assert.assertFalse(
        comparator.isEqual(field(OType.BOOLEAN, false), field(OType.STRING, "true")));
    Assert.assertFalse(
        comparator.isEqual(field(OType.BOOLEAN, true), field(OType.STRING, "false")));
  }

  @Test
  public void testBinaryFieldCopy() {
    final OBinaryField f = field(OType.BYTE, 10, new OCaseInsensitiveCollate()).copy();
    Assert.assertEquals(f.type, OType.BYTE);
    Assert.assertNotNull(f.bytes);
    Assert.assertEquals(f.collate.getName(), OCaseInsensitiveCollate.NAME);
  }

  @Test
  public void testBinaryComparable() {
    for (OType t : OType.values()) {
      switch (t) {
        case INTEGER:
        case LONG:
        case DATETIME:
        case SHORT:
        case STRING:
        case DOUBLE:
        case FLOAT:
        case BYTE:
        case BOOLEAN:
        case DATE:
        case BINARY:
        case LINK:
        case DECIMAL:
          Assert.assertTrue(comparator.isBinaryComparable(t));
          break;

        default:
          Assert.assertFalse(comparator.isBinaryComparable(t));
      }
    }
  }

  protected void testEquals(OType sourceType, Number value10AsSourceType) {
    OType[] numberTypes =
        new OType[] {OType.BYTE, OType.DOUBLE, OType.FLOAT, OType.SHORT, OType.INTEGER, OType.LONG};

    for (OType t : numberTypes) {
      if (sourceType == OType.DATETIME && t == OType.BYTE)
        // SKIP TEST
        continue;

      testEquals(sourceType, t);
    }

    for (OType t : numberTypes) {
      testEquals(t, sourceType);
    }

    if (sourceType != OType.DATETIME) {
      // STRING
      Assert.assertTrue(
          comparator.isEqual(
              field(sourceType, value10AsSourceType),
              field(OType.STRING, value10AsSourceType.toString())));
      Assert.assertFalse(
          comparator.isEqual(field(sourceType, value10AsSourceType), field(OType.STRING, "9")));
      Assert.assertFalse(
          comparator.isEqual(field(sourceType, value10AsSourceType), field(OType.STRING, "11")));
      Assert.assertFalse(
          comparator.isEqual(
              field(sourceType, value10AsSourceType.intValue() * 2), field(OType.STRING, "11")));

      Assert.assertTrue(
          comparator.isEqual(
              field(OType.STRING, value10AsSourceType.toString()),
              field(sourceType, value10AsSourceType)));
      Assert.assertFalse(
          comparator.isEqual(
              field(OType.STRING, value10AsSourceType.toString()),
              field(sourceType, value10AsSourceType.intValue() - 1)));
      Assert.assertFalse(
          comparator.isEqual(
              field(OType.STRING, value10AsSourceType.toString()),
              field(sourceType, value10AsSourceType.intValue() + 1)));
      Assert.assertFalse(
          comparator.isEqual(
              field(OType.STRING, "" + value10AsSourceType.intValue() * 2),
              field(sourceType, value10AsSourceType.intValue())));
    }
  }

  protected void testEquals(OType sourceType, OType destType) {
    try {
      Assert.assertTrue(comparator.isEqual(field(sourceType, 10), field(destType, 10)));
      Assert.assertFalse(comparator.isEqual(field(sourceType, 10), field(destType, 9)));
      Assert.assertFalse(comparator.isEqual(field(sourceType, 10), field(destType, 11)));
    } catch (AssertionError e) {
      System.out.println("ERROR: testEquals(" + sourceType + "," + destType + ")");
      System.out.flush();
      throw e;
    }
  }
}
