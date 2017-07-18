package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class DateBinaryComparatorTest {

  private final String dateFormat = "yyyy-MM-dd";
  private final String dbUrl      = "memory:DateBinaryComparatorTest";
  private final String dateValue  = "2017-07-18";

  private ODatabaseDocument db;

  @Before
  public void initDatabase() {
    db = new ODatabaseDocumentTx(dbUrl);
    if (!db.exists()) {
      db.create();
    }
    openDatabase();
    initSchema();
  }

  @After
  public void dropDatabase() {
    db.drop();
  }

  private void initSchema() {
    OClass testClass = db.getMetadata().getSchema().createClass("Test");
    testClass.createProperty("date", OType.DATE);
    ODocument document = new ODocument(testClass.getName());

    try {
      document.field("date", new SimpleDateFormat(dateFormat).parse(dateValue));
      document.save();
    } catch (ParseException e) {
      e.printStackTrace();
    }
    db.commit();
  }

  @Test
  public void testDateJavaClassPreparedStatement() throws ParseException {
    String str = "SELECT FROM Test WHERE date = :dateParam";
    OSQLSynchQuery query = new OSQLSynchQuery(str);
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("dateParam", new SimpleDateFormat(dateFormat).parse(dateValue));

    List<?> result = db.query(query, params);
    assertTrue(result.size() == 1);
  }

  private void openDatabase() {
    if (db.isClosed()) {
      db.open("admin", "admin");
    }
  }
}