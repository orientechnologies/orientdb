package com.orientechnologies.orient.core.record.impl;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSerializationCompatibilityRecord {

  private ODatabaseDocument database;

  @Before
  public void before() {
    database =
        new ODatabaseDocumentTx(
            "memory:" + TestSerializationCompatibilityRecord.class.getSimpleName());
    database.create();
  }

  @Test
  public void testDataNotMatchSchema() {
    OClass klass =
        database
            .getMetadata()
            .getSchema()
            .createClass("Test", database.getMetadata().getSchema().getClass("V"));
    ODocument doc = new ODocument("Test");
    Map<String, ORID> map = new HashMap<String, ORID>();
    map.put("some", new ORecordId(10, 20));
    doc.field("map", map, OType.LINKMAP);
    ORID id = database.save(doc).getIdentity();
    klass.createProperty("map", OType.EMBEDDEDMAP, (OType) null, true);
    database.getMetadata().reload();
    database.getLocalCache().clear();
    ODocument record = database.load(id);
    // Force deserialize + serialize;
    record.field("some", "aa");
    database.save(record);
    database.getLocalCache().clear();
    ODocument record1 = database.load(id);
    assertEquals(record1.fieldType("map"), OType.LINKMAP);
  }

  @After
  public void after() {
    database.drop();
  }
}
