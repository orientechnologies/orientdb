package com.orientechnologies.orient.core.sql.functions;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Luca Garulli
 */
public class OSQLFunctionConvertTest {

  @Test
  public void testSQLConversions() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:testSQLConvert");
    try {
      db.create();

      db.command(new OCommandSQL("create class TestConversion")).execute();

      db.command(new OCommandSQL("insert into TestConversion set string = 'Jay', date = sysdate(), number = 33")).execute();

      List<ODocument> results = db.query(new OSQLSynchQuery<ODocument>("select string.asString() as convert from TestConversion"));
      assertNotNull(results);
      assertEquals(results.size(), 1);
      assertTrue(results.get(0).field("convert") instanceof String);

      results = db.query(new OSQLSynchQuery<ODocument>("select number.asDate() as convert from TestConversion"));
      assertNotNull(results);
      assertEquals(results.size(), 1);
      assertTrue(results.get(0).field("convert") instanceof Date);

      results = db.query(new OSQLSynchQuery<ODocument>("select number.asDateTime() as convert from TestConversion"));
      assertNotNull(results);
      assertEquals(results.size(), 1);
      assertTrue(results.get(0).field("convert") instanceof Date);

      results = db.query(new OSQLSynchQuery<ODocument>("select number.asInteger() as convert from TestConversion"));
      assertNotNull(results);
      assertEquals(results.size(), 1);
      assertTrue(results.get(0).field("convert") instanceof Integer);

      results = db.query(new OSQLSynchQuery<ODocument>("select number.asLong() as convert from TestConversion"));
      assertNotNull(results);
      assertEquals(results.size(), 1);
      assertTrue(results.get(0).field("convert") instanceof Long);

      results = db.query(new OSQLSynchQuery<ODocument>("select number.asFloat() as convert from TestConversion"));
      assertNotNull(results);
      assertEquals(results.size(), 1);
      assertTrue(results.get(0).field("convert") instanceof Float);

      results = db.query(new OSQLSynchQuery<ODocument>("select number.asDecimal() as convert from TestConversion"));
      assertNotNull(results);
      assertEquals(results.size(), 1);
      assertTrue(results.get(0).field("convert") instanceof BigDecimal);

      results = db.query(new OSQLSynchQuery<ODocument>("select number.convert('LONG') as convert from TestConversion"));
      assertNotNull(results);
      assertEquals(results.size(), 1);
      assertTrue(results.get(0).field("convert") instanceof Long);

      results = db.query(new OSQLSynchQuery<ODocument>("select number.convert('SHORT') as convert from TestConversion"));
      assertNotNull(results);
      assertEquals(results.size(), 1);
      assertTrue(results.get(0).field("convert") instanceof Short);

      results = db.query(new OSQLSynchQuery<ODocument>("select number.convert('DOUBLE') as convert from TestConversion"));
      assertNotNull(results);
      assertEquals(results.size(), 1);
      assertTrue(results.get(0).field("convert") instanceof Double);

    } finally {
      db.drop();
    }
  }
}
