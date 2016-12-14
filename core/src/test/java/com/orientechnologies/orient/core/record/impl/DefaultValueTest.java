package com.orientechnologies.orient.core.record.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Date;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.util.ODateHelper;

public class DefaultValueTest {

  private ODatabaseDocument database;

  @Before
  public void before() {
    database = new ODatabaseDocumentTx("memory:" + DefaultValueTest.class.getSimpleName());
    database.create();
  }

  @After
  public void after() {
    database.drop();
  }

  @Test
  public void testKeepValueSerialization() {
    // create example schema
    OSchema schema = database.getMetadata().getSchema();
    OClass classA = schema.createClass("ClassC");

    OProperty prop = classA.createProperty("name", OType.STRING);
    prop.setDefaultValue("uuid()");

    ODocument doc = new ODocument("ClassC");

    byte[] val = doc.toStream();
    ODocument doc1 = new ODocument();
    doc1.fromStream(val);
    doc1.deserializeFields();
    assertEquals(doc.field("name"), doc1.field("name"));

  }

  @Test
  public void testDefaultValueDate() {
    OSchema schema = database.getMetadata().getSchema();
    OClass classA = schema.createClass("ClassA");

    OProperty prop = classA.createProperty("date", OType.DATE);
    prop.setDefaultValue(ODateHelper.getDateTimeFormatInstance().format(new Date()));
    OProperty some = classA.createProperty("id", OType.STRING);
    some.setDefaultValue("uuid()");

    System.out.println(prop.getDefaultValue());

    ODocument doc = new ODocument(classA);
    ODocument saved = database.save(doc);
    assertNotNull(saved.field("date"));
    assertTrue(saved.field("date") instanceof Date);
    assertNotNull(saved.field("id"));

    OIdentifiable id = database.command(new OCommandSQL("insert into ClassA content {}")).execute();
    ODocument seved1 = database.load(id.getIdentity());
    assertNotNull(seved1.field("date"));
    assertNotNull(seved1.field("id"));
    assertTrue(seved1.field("date") instanceof Date);

  }

}
