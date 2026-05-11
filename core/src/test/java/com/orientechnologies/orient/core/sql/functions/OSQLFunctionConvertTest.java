package com.orientechnologies.orient.core.sql.functions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import org.junit.Test;

/** @author Luca Garulli (l.garulli--(at)--orientdb.com) */
public class OSQLFunctionConvertTest extends BaseMemoryDatabase {

  @Test
  public void testSQLConversions() {

    db.command(new OCommandSQL("create class TestConversion")).execute();

    db.command(
            new OCommandSQL(
                "insert into TestConversion set string = 'Jay', date = sysdate(), number = 33"))
        .execute();

    ODocument doc =
        (ODocument)
            db.query(new OSQLSynchQuery<ODocument>("select from TestConversion limit 1")).get(0);

    db.command(
            new OCommandSQL("update TestConversion set selfrid = 'foo" + doc.getIdentity() + "'"))
        .execute();

    List<ODocument> results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select string.asString() as convert from TestConversion"));
    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(0).field("convert") instanceof String);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>("select number.asDate() as convert from TestConversion"));
    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(0).field("convert") instanceof Date);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select number.asDateTime() as convert from TestConversion"));
    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(0).field("convert") instanceof Date);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select number.asInteger() as convert from TestConversion"));
    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(0).field("convert") instanceof Integer);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>("select number.asLong() as convert from TestConversion"));
    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(0).field("convert") instanceof Long);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select number.asFloat() as convert from TestConversion"));
    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(0).field("convert") instanceof Float);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select number.asDecimal() as convert from TestConversion"));
    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(0).field("convert") instanceof BigDecimal);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select number.convert('LONG') as convert from TestConversion"));
    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(0).field("convert") instanceof Long);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select number.convert('SHORT') as convert from TestConversion"));
    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(0).field("convert") instanceof Short);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select number.convert('DOUBLE') as convert from TestConversion"));
    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(0).field("convert") instanceof Double);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select selfrid.substring(3).convert('LINK').string as convert from TestConversion"));
    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertEquals(results.get(0).field("convert"), "Jay");
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
