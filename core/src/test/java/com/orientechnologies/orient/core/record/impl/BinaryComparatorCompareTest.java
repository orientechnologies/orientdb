package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.collate.OCaseInsensitiveCollate;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.util.ODateHelper;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;

public class BinaryComparatorCompareTest extends AbstractComparatorTest {;

  @Test
  public void testInteger() {
    testCompareNumber(OType.INTEGER, 10);
  }

  @Test
  public void testLong() {
    testCompareNumber(OType.LONG, 10l);
  }

  @Test
  public void testShort() {
    testCompareNumber(OType.SHORT, (short) 10);
  }

  @Test
  public void testByte() {
    testCompareNumber(OType.BYTE, (byte) 10);
  }

  @Test
  public void testFloat() {
    testCompareNumber(OType.FLOAT, 10f);
  }

  @Test
  public void testDouble() {
    testCompareNumber(OType.DOUBLE, 10d);
  }

  @Test
  public void testDatetime() throws ParseException {
    testCompareNumber(OType.DATETIME, 10l);

    final SimpleDateFormat format =
        new SimpleDateFormat(OStorageConfiguration.DEFAULT_DATETIME_FORMAT);
    format.setTimeZone(ODateHelper.getDatabaseTimeZone());

    String now1 = format.format(new Date());
    Date now = format.parse(now1);

    Assert.assertEquals(
        comparator.compare(field(OType.DATETIME, now), field(OType.STRING, format.format(now))), 0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.DATETIME, new Date(now.getTime() + 1)),
                field(OType.STRING, format.format(now)))
            > 0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.DATETIME, new Date(now.getTime() - 1)),
                field(OType.STRING, format.format(now)))
            < 0);
  }

  @Test
  public void testBinary() throws ParseException {
    final byte[] b1 = new byte[] {0, 1, 2, 3};
    final byte[] b2 = new byte[] {0, 1, 2, 4};
    final byte[] b3 = new byte[] {1, 1, 2, 4};

    Assert.assertTrue(
        "For values " + field(OType.BINARY, b1) + " and " + field(OType.BINARY, b1),
        comparator.compare(field(OType.BINARY, b1), field(OType.BINARY, b1)) == 0);
    Assert.assertFalse(comparator.compare(field(OType.BINARY, b1), field(OType.BINARY, b2)) > 1);
    Assert.assertFalse(comparator.compare(field(OType.BINARY, b1), field(OType.BINARY, b3)) > 1);
  }

  @Test
  public void testLinks() throws ParseException {
    Assert.assertTrue(
        comparator.compare(
                field(OType.LINK, new ORecordId(1, 2)), field(OType.LINK, new ORecordId(1, 2)))
            == 0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.LINK, new ORecordId(1, 2)), field(OType.LINK, new ORecordId(2, 1)))
            < 0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.LINK, new ORecordId(1, 2)), field(OType.LINK, new ORecordId(0, 2)))
            > 0);

    Assert.assertTrue(
        comparator.compare(
                field(OType.LINK, new ORecordId(1, 2)),
                field(OType.STRING, new ORecordId(1, 2).toString()))
            == 0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.LINK, new ORecordId(1, 2)),
                field(OType.STRING, new ORecordId(0, 2).toString()))
            > 0);
  }

  @Test
  public void testString() {
    Assert.assertEquals(
        comparator.compare(field(OType.STRING, "test"), field(OType.STRING, "test")), 0);
    Assert.assertTrue(
        comparator.compare(field(OType.STRING, "test2"), field(OType.STRING, "test")) > 0);
    Assert.assertTrue(
        comparator.compare(field(OType.STRING, "test"), field(OType.STRING, "test2")) < 0);
    Assert.assertTrue(comparator.compare(field(OType.STRING, "t"), field(OType.STRING, "te")) < 0);

    // DEF COLLATE
    Assert.assertEquals(
        comparator.compare(
            field(OType.STRING, "test", new ODefaultCollate()), field(OType.STRING, "test")),
        0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.STRING, "test2", new ODefaultCollate()), field(OType.STRING, "test"))
            > 0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.STRING, "test", new ODefaultCollate()), field(OType.STRING, "test2"))
            < 0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.STRING, "t", new ODefaultCollate()), field(OType.STRING, "te"))
            < 0);

    Assert.assertEquals(
        comparator.compare(
            field(OType.STRING, "test", new ODefaultCollate()),
            field(OType.STRING, "test", new ODefaultCollate())),
        0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.STRING, "test2", new ODefaultCollate()),
                field(OType.STRING, "test", new ODefaultCollate()))
            > 0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.STRING, "test", new ODefaultCollate()),
                field(OType.STRING, "test2", new ODefaultCollate()))
            < 0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.STRING, "t", new ODefaultCollate()),
                field(OType.STRING, "te", new ODefaultCollate()))
            < 0);

    Assert.assertEquals(
        comparator.compare(
            field(OType.STRING, "test"), field(OType.STRING, "test", new ODefaultCollate())),
        0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.STRING, "test2"), field(OType.STRING, "test", new ODefaultCollate()))
            > 0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.STRING, "test"), field(OType.STRING, "test2", new ODefaultCollate()))
            < 0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.STRING, "t"), field(OType.STRING, "te", new ODefaultCollate()))
            < 0);

    // CASE INSENSITIVE COLLATE
    Assert.assertEquals(
        comparator.compare(
            field(OType.STRING, "test"),
            field(OType.STRING, "test", new OCaseInsensitiveCollate())),
        0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.STRING, "test2"),
                field(OType.STRING, "test", new OCaseInsensitiveCollate()))
            > 0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.STRING, "test"),
                field(OType.STRING, "test2", new OCaseInsensitiveCollate()))
            < 0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.STRING, "t"), field(OType.STRING, "te", new OCaseInsensitiveCollate()))
            < 0);

    Assert.assertEquals(
        comparator.compare(
            field(OType.STRING, "test"),
            field(OType.STRING, "TEST", new OCaseInsensitiveCollate())),
        0);
    Assert.assertEquals(
        comparator.compare(
            field(OType.STRING, "TEST"),
            field(OType.STRING, "TEST", new OCaseInsensitiveCollate())),
        0);
    Assert.assertEquals(
        comparator.compare(
            field(OType.STRING, "TE"), field(OType.STRING, "te", new OCaseInsensitiveCollate())),
        0);

    Assert.assertTrue(
        comparator.compare(
                field(OType.STRING, "test2"),
                field(OType.STRING, "TEST", new OCaseInsensitiveCollate()))
            > 0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.STRING, "test"),
                field(OType.STRING, "TEST2", new OCaseInsensitiveCollate()))
            < 0);
    Assert.assertTrue(
        comparator.compare(
                field(OType.STRING, "t"), field(OType.STRING, "tE", new OCaseInsensitiveCollate()))
            < 0);
  }

  @Test
  public void testDecimal() {
    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.DECIMAL, new BigDecimal(10))),
        0);
    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.DECIMAL, new BigDecimal(11))),
        -1);
    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.DECIMAL, new BigDecimal(9))),
        1);

    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.SHORT, new Short((short) 10))),
        0);
    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.SHORT, new Short((short) 11))),
        -1);
    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.SHORT, new Short((short) 9))),
        1);

    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.INTEGER, new Integer(10))),
        0);
    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.INTEGER, new Integer(11))),
        -1);
    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.INTEGER, new Integer(9))),
        1);

    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.LONG, new Long(10))),
        0);
    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.LONG, new Long(11))),
        -1);
    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.LONG, new Long(9))),
        1);

    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.FLOAT, new Float(10))),
        0);
    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.FLOAT, new Float(11))),
        -1);
    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.FLOAT, new Float(9))),
        1);

    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.DOUBLE, new Double(10))),
        0);
    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.DOUBLE, new Double(11))),
        -1);
    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.DOUBLE, new Double(9))),
        1);

    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.BYTE, new Byte((byte) 10))),
        0);
    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.BYTE, new Byte((byte) 11))),
        -1);
    Assert.assertEquals(
        comparator.compare(
            field(OType.DECIMAL, new BigDecimal(10)), field(OType.BYTE, new Byte((byte) 9))),
        1);

    Assert.assertEquals(
        comparator.compare(field(OType.DECIMAL, new BigDecimal(10)), field(OType.STRING, "10")), 0);
    Assert.assertTrue(
        comparator.compare(field(OType.DECIMAL, new BigDecimal(10)), field(OType.STRING, "11"))
            < 0);
    Assert.assertTrue(
        comparator.compare(field(OType.DECIMAL, new BigDecimal(10)), field(OType.STRING, "9")) < 0);
    Assert.assertTrue(
        comparator.compare(field(OType.DECIMAL, new BigDecimal(20)), field(OType.STRING, "11"))
            > 0);
  }

  @Test
  public void testBoolean() {
    Assert.assertEquals(
        comparator.compare(field(OType.BOOLEAN, true), field(OType.BOOLEAN, true)), 0);
    Assert.assertEquals(
        comparator.compare(field(OType.BOOLEAN, true), field(OType.BOOLEAN, false)), 1);
    Assert.assertEquals(
        comparator.compare(field(OType.BOOLEAN, false), field(OType.BOOLEAN, true)), -1);

    Assert.assertTrue(
        comparator.compare(field(OType.BOOLEAN, true), field(OType.STRING, "true")) == 0);
    Assert.assertTrue(
        comparator.compare(field(OType.BOOLEAN, false), field(OType.STRING, "false")) == 0);
    Assert.assertTrue(
        comparator.compare(field(OType.BOOLEAN, false), field(OType.STRING, "true")) < 0);
    Assert.assertTrue(
        comparator.compare(field(OType.BOOLEAN, true), field(OType.STRING, "false")) > 0);
  }

  protected void testCompareNumber(OType sourceType, Number value10AsSourceType) {
    OType[] numberTypes =
        new OType[] {
          OType.BYTE,
          OType.DOUBLE,
          OType.FLOAT,
          OType.SHORT,
          OType.INTEGER,
          OType.LONG,
          OType.DATETIME
        };

    for (OType t : numberTypes) {
      if (sourceType == OType.DATETIME && t == OType.BYTE)
        // SKIP TEST
        continue;

      testCompare(sourceType, t);
    }

    for (OType t : numberTypes) {
      testCompare(t, sourceType);
    }

    // STRING
    if (sourceType != OType.DATETIME) {
      Assert.assertEquals(
          comparator.compare(
              field(sourceType, value10AsSourceType),
              field(OType.STRING, value10AsSourceType.toString())),
          0);
      Assert.assertTrue(
          comparator.compare(field(sourceType, value10AsSourceType), field(OType.STRING, "9")) < 0);
      Assert.assertTrue(
          comparator.compare(field(sourceType, value10AsSourceType), field(OType.STRING, "11"))
              < 0);
      Assert.assertTrue(
          comparator.compare(
                  field(sourceType, value10AsSourceType.intValue() * 2), field(OType.STRING, "11"))
              > 0);

      Assert.assertEquals(
          comparator.compare(
              field(OType.STRING, value10AsSourceType.toString()),
              field(sourceType, value10AsSourceType)),
          0);
      Assert.assertTrue(
          comparator.compare(
                  field(OType.STRING, value10AsSourceType.toString()),
                  field(sourceType, value10AsSourceType.intValue() - 1))
              < 0);
      Assert.assertTrue(
          comparator.compare(
                  field(OType.STRING, value10AsSourceType.toString()),
                  field(sourceType, value10AsSourceType.intValue() + 1))
              < 0);
      Assert.assertTrue(
          comparator.compare(
                  field(OType.STRING, "" + value10AsSourceType.intValue() * 2),
                  field(sourceType, value10AsSourceType.intValue()))
              > 0);
    }
  }

  protected void testCompare(OType sourceType, OType destType) {
    testCompare(sourceType, destType, 10);
  }

  protected void testCompare(OType sourceType, OType destType, final Number value) {
    try {
      Assert.assertEquals(comparator.compare(field(sourceType, value), field(destType, value)), 0);
      Assert.assertEquals(
          comparator.compare(field(sourceType, value), field(destType, value.intValue() - 1)), 1);
      Assert.assertEquals(
          comparator.compare(field(sourceType, value), field(destType, value.intValue() + 1)), -1);
    } catch (AssertionError e) {
      System.out.println("ERROR: testCompare(" + sourceType + "," + destType + "," + value + ")");
      System.out.flush();
      throw e;
    }
  }
}
