package com.orientechnologies.orient.core.record.impl;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.junit.Test;

/** Created by tglman on 25/01/16. */
public class OJsonWithCustom {

  @Test
  public void testCustomField() {
    boolean old = OGlobalConfiguration.DB_CUSTOM_SUPPORT.getValueAsBoolean();
    OGlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(true);
    ODocument doc = new ODocument();
    doc.field("test", String.class, OType.CUSTOM);

    String json = doc.toJSON();

    System.out.println(json);

    ODocument doc1 = new ODocument();
    doc1.fromJSON(json);
    assertEquals(doc.<String>field("test"), doc1.field("test"));
    OGlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(old);
  }

  @Test(expected = ODatabaseException.class)
  public void testCustomFieldDisabled() {
    ODocument doc = new ODocument();
    doc.field("test", String.class, OType.CUSTOM);

    String json = doc.toJSON();

    System.out.println(json);

    ODocument doc1 = new ODocument();
    doc1.fromJSON(json);
    assertEquals(doc.<String>field("test"), doc1.field("test"));
  }

  @Test
  public void testCustomSerialization() {
    boolean old = OGlobalConfiguration.DB_CUSTOM_SUPPORT.getValueAsBoolean();
    OGlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(true);
    try (final OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase(
            "testJson", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY)) {
      // orientDB.create("testJson", ODatabaseType.MEMORY);
      final ODatabaseSession db =
          orientDB.open("testJson", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      try {
        OClass klass = db.getMetadata().getSchema().createClass("TestCustom");
        klass.createProperty("test", OType.CUSTOM);
        ODocument doc = new ODocument("TestCustom");
        doc.field("test", TestCustom.ONE, OType.CUSTOM);

        String json = doc.toJSON();

        ODocument doc1 = new ODocument();
        doc1.fromJSON(json);
        assertEquals(TestCustom.valueOf((String) doc1.field("test")), TestCustom.ONE);
      } finally {
        db.close();
      }
    }
    OGlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(old);
  }

  public enum TestCustom {
    ONE,
    TWO
  }
}
