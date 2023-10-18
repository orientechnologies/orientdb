package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.junit.Assert;
import org.junit.Test;

public class DefaultValueSerializationTest extends BaseMemoryDatabase {

  @Test
  public void testKeepValueSerialization() {
    // create example schema
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("ClassC");

    OProperty prop = classA.createProperty("name", OType.STRING);
    prop.setDefaultValue("uuid()");

    ODocument doc = new ODocument("ClassC");

    byte[] val = doc.toStream();
    ODocument doc1 = new ODocument();
    doc1.fromStream(val);
    doc1.deserializeFields();
    Assert.assertEquals(doc.field("name").toString(), doc1.field("name").toString());
  }
}
