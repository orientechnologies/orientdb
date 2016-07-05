package com.orientechnologies.orient.core.db.document;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class ODatabaseDocumentTxTest {

  private ODatabaseDocumentTx db;

  @Before public void setUp() throws Exception {
    String url = "memory:" + ODatabaseDocumentTxTest.class.getSimpleName();
    db = new ODatabaseDocumentTx(url).create();

  }

  @After public void tearDown() throws Exception {
    db.drop();
  }

  @Test public void testMultipleReads() {

    db.getMetadata().getSchema().createClass("TestMultipleRead1");
    db.getMetadata().getSchema().createClass("TestMultipleRead2");

    final HashSet<ORecordId> rids = new HashSet<ORecordId>();

    for (int i = 0; i < 100; ++i) {
      final ODocument rec = new ODocument("TestMultipleRead1").field("id", i).save();
      rids.add((ORecordId) rec.getIdentity());

      final ODocument rec2 = new ODocument("TestMultipleRead2").field("id", i).save();
      rids.add((ORecordId) rec2.getIdentity());
    }

    Set<ORecord> result = db.executeReadRecords(rids, false);
    Assert.assertEquals(result.size(), 200);

    for (ORecord rec : result) {
      Assert.assertTrue(rec instanceof ODocument);
    }

    Set<ORecord> result2 = db.executeReadRecords(rids, true);
    Assert.assertEquals(result2.size(), 200);

    for (ORecord rec : result2) {
      Assert.assertTrue(rec instanceof ODocument);
    }

  }

  @Test public void testCountClass() throws Exception {

    OClass testSuperclass = db.getMetadata().getSchema().createClass("TestSuperclass");
    db.getMetadata().getSchema().createClass("TestSubclass", testSuperclass);

    ODocument toDelete = new ODocument("TestSubclass").field("id", 1).save();

    // 1 SUB, 0 SUPER
    Assert.assertEquals(db.countClass("TestSubclass", false), 1);
    Assert.assertEquals(db.countClass("TestSubclass", true), 1);
    Assert.assertEquals(db.countClass("TestSuperclass", false), 0);
    Assert.assertEquals(db.countClass("TestSuperclass", true), 1);

    db.begin();
    try {
      new ODocument("TestSuperclass").field("id", 1).save();
      new ODocument("TestSubclass").field("id", 1).save();
      // 2 SUB, 1 SUPER

      Assert.assertEquals(db.countClass("TestSuperclass", false), 1);
      Assert.assertEquals(db.countClass("TestSuperclass", true), 3);
      Assert.assertEquals(db.countClass("TestSubclass", false), 2);
      Assert.assertEquals(db.countClass("TestSubclass", true), 2);

      toDelete.delete().save();
      // 1 SUB, 1 SUPER

      Assert.assertEquals(db.countClass("TestSuperclass", false), 1);
      Assert.assertEquals(db.countClass("TestSuperclass", true), 2);
      Assert.assertEquals(db.countClass("TestSubclass", false), 1);
      Assert.assertEquals(db.countClass("TestSubclass", true), 1);
    } finally {
      db.commit();
    }

  }

  @Test public void testTimezone() {

    db.set(ODatabase.ATTRIBUTES.TIMEZONE, "Europe/Rome");
    Object newTimezone = db.get(ODatabase.ATTRIBUTES.TIMEZONE);
    Assert.assertEquals(newTimezone, "Europe/Rome");

    db.set(ODatabase.ATTRIBUTES.TIMEZONE, "foobar");
    newTimezone = db.get(ODatabase.ATTRIBUTES.TIMEZONE);
    Assert.assertEquals(newTimezone, "GMT");
  }

  @Test(expected = ODatabaseException.class) public void testSaveInvalidRid() {
    ODocument doc = new ODocument();

    doc.field("test", new ORecordId(-2, 10));

    db.save(doc);
  }

  @Test public void testCreateClass() {
    OClass clazz = db.createClass("TestCreateClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateClass", clazz.getName());
    List<OClass> superclasses = clazz.getSuperClasses();
    if (superclasses != null) {
      Assert.assertTrue(superclasses.isEmpty());
    }
    Assert.assertNotNull(db.getMetadata().getSchema().getClass("TestCreateClass"));
    try {
      db.createClass("TestCreateClass");
      Assert.fail();
    } catch (OSchemaException ex) {
    }

    OClass subclazz = db.createClass("TestCreateClass_subclass", "TestCreateClass");
    Assert.assertNotNull(subclazz );
    Assert.assertEquals("TestCreateClass_subclass", subclazz .getName());
    List<OClass> sub_superclasses = subclazz .getSuperClasses();
    Assert.assertEquals(1, sub_superclasses.size());
    Assert.assertEquals("TestCreateClass", sub_superclasses.get(0).getName());

  }

  @Test public void testGetClass() {
    db.createClass("TestGetClass");

    OClass clazz = db.getClass("TestGetClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestGetClass", clazz.getName());
    List<OClass> superclasses = clazz.getSuperClasses();
    if (superclasses != null) {
      Assert.assertTrue(superclasses.isEmpty());
    }

    OClass clazz2 = db.getClass("TestGetClass_non_existing");
    Assert.assertNull(clazz2);
  }

  @Test
  public void testDocFromJsonEmbedded() {
      OSchema schema = db.getMetadata().getSchema();

      OClass c0 = schema.createClass("testDocFromJsonEmbedded_Class0");

      OClass c1 = schema.createClass("testDocFromJsonEmbedded_Class1");
      c1.createProperty("account", OType.STRING);
      c1.createProperty("meta", OType.EMBEDDED, c0);

      ODocument doc = new ODocument("testDocFromJsonEmbedded_Class1");

      doc.fromJSON("{\n" + "    \"account\": \"#25:0\",\n" + "    "
          + "\"meta\": {"
          + "   \"created\": \"2016-10-03T21:10:21.77-07:00\",\n" + "        \"ip\": \"0:0:0:0:0:0:0:1\",\n"
          + "   \"contentType\": \"application/x-www-form-urlencoded\","
          + "   \"userAgent\": \"PostmanRuntime/2.5.2\""
          + "},"
          + "\"data\": \"firstName=Jessica&lastName=Smith\"\n" + "}");

      db.save(doc);

      List<ODocument> result = db.query(new OSQLSynchQuery<Object>("select from testDocFromJsonEmbedded_Class0"));
      Assert.assertEquals(result.size(), 0);

      result = db.query(new OSQLSynchQuery<Object>("select from testDocFromJsonEmbedded_Class1"));
      Assert.assertEquals(result.size(), 1);
      ODocument item = result.get(0);
      ODocument meta = item.field("meta");
      Assert.assertEquals(meta.getClassName(), "testDocFromJsonEmbedded_Class0");
      Assert.assertEquals(meta.field("ip"), "0:0:0:0:0:0:0:1");

  }

  @Test public void testCreateClassIfNotExists() {
    db.createClass("TestCreateClassIfNotExists");

    OClass clazz = db.createClassIfNotExist("TestCreateClassIfNotExists");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateClassIfNotExists", clazz.getName());
    List<OClass> superclasses = clazz.getSuperClasses();
    if (superclasses != null) {
      Assert.assertTrue(superclasses.isEmpty());
    }

    OClass clazz2 = db.createClassIfNotExist("TestCreateClassIfNotExists_non_existing");
    Assert.assertNotNull(clazz2);
    Assert.assertEquals("TestCreateClassIfNotExists_non_existing", clazz2.getName());
    List<OClass> superclasses2 = clazz2.getSuperClasses();
    if (superclasses2 != null) {
      Assert.assertTrue(superclasses2.isEmpty());
    }
  }

  @Test public void testCreateVertexClass() {
    OClass clazz = db.createVertexClass("TestCreateVertexClass");
    Assert.assertNotNull(clazz);

    clazz = db.getClass("TestCreateVertexClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateVertexClass", clazz.getName());
    List<OClass> superclasses = clazz.getSuperClasses();
    Assert.assertEquals(1, superclasses.size());
    Assert.assertEquals("V", superclasses.get(0).getName());
  }

  @Test public void testCreateEdgeClass() {
    OClass clazz = db.createEdgeClass("TestCreateEdgeClass");
    Assert.assertNotNull(clazz);

    clazz = db.getClass("TestCreateEdgeClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateEdgeClass", clazz.getName());
    List<OClass> superclasses = clazz.getSuperClasses();
    Assert.assertEquals(1, superclasses.size());
    Assert.assertEquals("E", superclasses.get(0).getName());
  }

}
