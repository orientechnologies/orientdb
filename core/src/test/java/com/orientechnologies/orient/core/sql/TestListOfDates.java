package com.orientechnologies.orient.core.sql;

/**
 * Created by luigidellaquila on 20/07/17.
 */

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
import java.util.*;

import static org.junit.Assert.assertTrue;

public class TestListOfDates {

  private final String className      = "TestListOfDates";
  private final String dateField      = "date";
  private final String dateTimeField  = "dateTime";
  private final String dateFormat     = "yyyy-MM-dd";
  private final String dateTimeFormat = "yyyy-MM-dd HH:mm:ss";
  private final String dbUrl          = "memory:TestListOfDates";
  private final String dateValue      = "2017-07-17";
  private final String dateTimeValue  = "2017-07-17 23:09:00";

  private ODatabaseDocument db;

  @Before
  public void initDatabase() {
    db = new ODatabaseDocumentTx(dbUrl);
    if (!db.exists()) {
      db.create();
    }
    openDatabase();
    createClass();

  }

  @After
  public void dropDatabase() {
    openDatabase();
    db.drop();
  }

  private void createClass() {
    OClass testClass = db.getMetadata().getSchema().createClass(className);
    testClass.createProperty(dateField, OType.DATE);
    testClass.createProperty(dateTimeField, OType.DATETIME);
    ODocument document = new ODocument(testClass.getName());

    try {
      document.field(dateField, new SimpleDateFormat(dateFormat).parse(dateValue));
      document.field(dateTimeField, new SimpleDateFormat(dateTimeFormat).parse(dateTimeValue));
      document.save();
    } catch (ParseException e) {
      e.printStackTrace();
    }
    db.commit();
  }

  @Test
  public void testDateTimeCollectionPreparedStatement() throws ParseException {
    openDatabase();
    OSQLSynchQuery<List<ODocument>> query = new OSQLSynchQuery(
        String.format("SELECT FROM %s WHERE %s IN :%s", className, dateTimeField, dateTimeField));
    Map<String, Object> params = new HashMap();
    Date date = new SimpleDateFormat(dateTimeFormat).parse(dateTimeValue);
    params.put(dateTimeField, Arrays.asList(date));
    assertTrue(db.query(query, params).size() == 1);
  }

  @Test
  public void testDateCollectionPreparedStatement() throws ParseException {
    openDatabase();
    OSQLSynchQuery<List<ODocument>> query = new OSQLSynchQuery(
        String.format("SELECT FROM %s WHERE %s IN :%s", className, dateField, dateField));

    Map<String, Object> params = new HashMap();
    Date date = new SimpleDateFormat(dateFormat).parse(dateValue);
    params.put(dateField, Arrays.asList(date));
    assertTrue(db.query(query, params).size() == 1);
  }

  private void openDatabase() {
    if (db.isClosed()) {
      db.open("admin", "admin");
    }
  }
}