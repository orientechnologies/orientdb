package com.orientechnologies.orient.core.record.impl;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class TestSerializationCompatibilityRecord extends BaseMemoryDatabase {

  @Test
  public void testDataNotMatchSchema() {
    OClass klass =
        db.getMetadata()
            .getSchema()
            .createClass("Test", db.getMetadata().getSchema().getClass("V"));
    ODocument doc = new ODocument("Test");
    Map<String, ORID> map = new HashMap<String, ORID>();
    map.put("some", new ORecordId(10, 20));
    doc.field("map", map, OType.LINKMAP);
    ORID id = db.save(doc).getIdentity();
    klass.createProperty("map", OType.EMBEDDEDMAP, (OType) null, true);
    db.getMetadata().reload();
    db.getLocalCache().clear();
    ODocument record = db.load(id);
    // Force deserialize + serialize;
    record.field("some", "aa");
    db.save(record);
    db.getLocalCache().clear();
    ODocument record1 = db.load(id);
    assertEquals(record1.fieldType("map"), OType.LINKMAP);
  }
}
