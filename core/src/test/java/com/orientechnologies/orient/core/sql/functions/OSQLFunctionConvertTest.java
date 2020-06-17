package com.orientechnologies.orient.core.sql.functions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import org.junit.Test;

/** @author Luca Garulli (l.garulli--(at)--orientdb.com) */
public class OSQLFunctionConvertTest {

  @Test
  public void testSQLConversions() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:testSQLConvert");
    try {
      db.create();

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
              new OSQLSynchQuery<ODocument>(
                  "select number.asDate() as convert from TestConversion"));
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
              new OSQLSynchQuery<ODocument>(
                  "select number.asLong() as convert from TestConversion"));
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

    } finally {
      db.drop();
    }
  }
}
