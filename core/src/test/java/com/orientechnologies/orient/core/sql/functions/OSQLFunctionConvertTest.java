package com.orientechnologies.orient.core.sql.functions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import org.junit.Test;

/** @author Luca Garulli (l.garulli--(at)--orientdb.com) */
public class OSQLFunctionConvertTest extends BaseMemoryDatabase {

  @Test
  public void testSQLConversions() {

    db.command("create class TestConversion").close();

    db.command("insert into TestConversion set string = 'Jay', date = sysdate(), number = 33")
        .close();

    ORID doc = db.query("select from TestConversion limit 1").next().getIdentity().get();

    db.command("update TestConversion set selfrid = 'foo" + doc.getIdentity() + "'").close();

    OResultSet results = db.query("select string.asString() as convert from TestConversion");

    assertTrue(results.next().getProperty("convert") instanceof String);
    assertFalse(results.hasNext());

    results = db.query("select number.asDate() as convert from TestConversion");
    assertTrue(results.next().getProperty("convert") instanceof Date);
    assertFalse(results.hasNext());

    results = db.query("select number.asDateTime() as convert from TestConversion");
    assertTrue(results.next().getProperty("convert") instanceof Date);
    assertFalse(results.hasNext());

    results = db.query("select number.asInteger() as convert from TestConversion");
    assertTrue(results.next().getProperty("convert") instanceof Integer);
    assertFalse(results.hasNext());

    results = db.query("select number.asLong() as convert from TestConversion");
    assertTrue(results.next().getProperty("convert") instanceof Long);
    assertFalse(results.hasNext());

    results = db.query("select number.asFloat() as convert from TestConversion");
    assertTrue(results.next().getProperty("convert") instanceof Float);
    assertFalse(results.hasNext());

    results = db.query("select number.asDecimal() as convert from TestConversion");
    assertTrue(results.next().getProperty("convert") instanceof BigDecimal);
    assertFalse(results.hasNext());

    results = db.query("select number.convert('LONG') as convert from TestConversion");
    assertTrue(results.next().getProperty("convert") instanceof Long);
    assertFalse(results.hasNext());

    results = db.query("select number.convert('SHORT') as convert from TestConversion");
    assertTrue(results.next().getProperty("convert") instanceof Short);
    assertFalse(results.hasNext());

    results = db.query("select number.convert('DOUBLE') as convert from TestConversion");
    assertNotNull(results);
    assertTrue(results.next().getProperty("convert") instanceof Double);
    assertFalse(results.hasNext());

    results =
        db.query(
            "select selfrid.substring(3).convert('LINK').string as convert from TestConversion");
    assertEquals(results.next().getProperty("convert"), "Jay");
    assertFalse(results.hasNext());
  }

  @Test
  public void testAsDateTruncatesAfternoonTimestampsToMidnight() {
    db.command("create class TestAsDate").close();

    Date input = dateAt(2024, Calendar.JANUARY, 2, 15, 30, 45, 123);
    db.command("insert into TestAsDate set date = ?, millis = ?", input, input.getTime()).close();

    Date expected = dateAt(2024, Calendar.JANUARY, 2, 0, 0, 0, 0);

    try (OResultSet results =
        db.query(
            "select date.asDate() as dateConvert, millis.asDate() as millisConvert from"
                + " TestAsDate")) {
      OResult result = results.next();
      assertEquals(expected, result.getProperty("dateConvert"));
      assertEquals(expected, result.getProperty("millisConvert"));
      assertFalse(results.hasNext());
    }
  }

  private static Date dateAt(
      int year, int month, int day, int hour, int minute, int second, int millisecond) {
    Calendar calendar = new GregorianCalendar();
    calendar.clear();
    calendar.set(year, month, day, hour, minute, second);
    calendar.set(Calendar.MILLISECOND, millisecond);
    return calendar.getTime();
  }
}
