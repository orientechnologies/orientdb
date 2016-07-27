package com.orientechnologies.orient.core.record.impl;

import static org.testng.AssertJUnit.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;

public class DefaultValueSerializationTest {

  private ODatabaseDocument database;

  @Before
  public void before() {
    database = new ODatabaseDocumentTx("memory:" + DefaultValueSerializationTest.class.getSimpleName());
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

}
