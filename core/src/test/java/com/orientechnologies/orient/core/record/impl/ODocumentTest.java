package com.orientechnologies.orient.core.record.impl;

import org.testng.annotations.Test;

import java.util.*;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;

import static org.testng.Assert.*;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class ODocumentTest {
  @Test
  public void testCopyToCopiesEmptyFieldsTypesAndOwners() throws Exception {
    ODocument doc1 = new ODocument();

    ODocument doc2 = new ODocument().field("integer2", 123).field("string", "OrientDB").field("a", 123.3)

        .setFieldType("integer", OType.INTEGER).setFieldType("string", OType.STRING).setFieldType("binary", OType.BINARY);
    ODocumentInternal.addOwner(doc2, new ODocument());

    assertEquals(doc2.field("integer2"), 123);
    assertEquals(doc2.field("string"), "OrientDB");
    assertEquals(doc2.field("a"), 123.3);

    assertEquals(doc2.fieldType("integer"), OType.INTEGER);
    assertEquals(doc2.fieldType("string"), OType.STRING);
    assertEquals(doc2.fieldType("binary"), OType.BINARY);

    assertNotNull(doc2.getOwner());

    doc1.copyTo(doc2);

    assertEquals(doc2.field("integer2"), null);
    assertEquals(doc2.field("string"), null);
    assertEquals(doc2.field("a"), null);

    assertEquals(doc2.fieldType("integer"), null);
    assertEquals(doc2.fieldType("string"), null);
    assertEquals(doc2.fieldType("binary"), null);

    assertNull(doc2.getOwner());
  }

  @Test
  public void testNullConstructor() {
    new ODocument((String) null);
    new ODocument((OClass) null);
  }

  @Test
  public void testClearResetsFieldTypes() throws Exception {
    ODocument doc = new ODocument();
    doc.setFieldType("integer", OType.INTEGER);
    doc.setFieldType("string", OType.STRING);
    doc.setFieldType("binary", OType.BINARY);

    assertEquals(doc.fieldType("integer"), OType.INTEGER);
    assertEquals(doc.fieldType("string"), OType.STRING);
    assertEquals(doc.fieldType("binary"), OType.BINARY);

    doc.clear();

    assertEquals(doc.fieldType("integer"), null);
    assertEquals(doc.fieldType("string"), null);
    assertEquals(doc.fieldType("binary"), null);
  }

  @Test
  public void testResetResetsFieldTypes() throws Exception {
    ODocument doc = new ODocument();
    doc.setFieldType("integer", OType.INTEGER);
    doc.setFieldType("string", OType.STRING);
    doc.setFieldType("binary", OType.BINARY);

    assertEquals(doc.fieldType("integer"), OType.INTEGER);
    assertEquals(doc.fieldType("string"), OType.STRING);
    assertEquals(doc.fieldType("binary"), OType.BINARY);

    doc.reset();

    assertEquals(doc.fieldType("integer"), null);
    assertEquals(doc.fieldType("string"), null);
    assertEquals(doc.fieldType("binary"), null);
  }

  @Test
  public void testKeepFieldType() throws Exception {
    ODocument doc = new ODocument();
    doc.field("integer", 10, OType.INTEGER);
    doc.field("string", 20, OType.STRING);
    doc.field("binary", new byte[] { 30 }, OType.BINARY);

    assertEquals(doc.fieldType("integer"), OType.INTEGER);
    assertEquals(doc.fieldType("string"), OType.STRING);
    assertEquals(doc.fieldType("binary"), OType.BINARY);
  }

  @Test
  public void testKeepFieldTypeSerialization() throws Exception {
    ODocument doc = new ODocument();
    doc.field("integer", 10, OType.INTEGER);
    doc.field("link", new ORecordId(1, 2), OType.LINK);
    doc.field("string", 20, OType.STRING);
    doc.field("binary", new byte[] { 30 }, OType.BINARY);

    assertEquals(doc.fieldType("integer"), OType.INTEGER);
    assertEquals(doc.fieldType("link"), OType.LINK);
    assertEquals(doc.fieldType("string"), OType.STRING);
    assertEquals(doc.fieldType("binary"), OType.BINARY);
    ORecordSerializer ser = ODatabaseDocumentTx.getDefaultSerializer();
    byte[] bytes = ser.toStream(doc, false);
    doc = new ODocument();
    ser.fromStream(bytes, doc, null);
    assertEquals(doc.fieldType("integer"), OType.INTEGER);
    assertEquals(doc.fieldType("string"), OType.STRING);
    assertEquals(doc.fieldType("binary"), OType.BINARY);
    assertEquals(doc.fieldType("link"), OType.LINK);
  }

  @Test
  public void testKeepAutoFieldTypeSerialization() throws Exception {
    ODocument doc = new ODocument();
    doc.field("integer", 10);
    doc.field("link", new ORecordId(1, 2));
    doc.field("string", "string");
    doc.field("binary", new byte[] { 30 });

    // this is null because is not set on value set.
    assertNull(doc.fieldType("integer"));
    assertNull(doc.fieldType("link"));
    assertNull(doc.fieldType("string"));
    assertNull(doc.fieldType("binary"));
    ORecordSerializer ser = ODatabaseDocumentTx.getDefaultSerializer();
    byte[] bytes = ser.toStream(doc, false);
    doc = new ODocument();
    ser.fromStream(bytes, doc, null);
    assertEquals(doc.fieldType("integer"), OType.INTEGER);
    assertEquals(doc.fieldType("string"), OType.STRING);
    assertEquals(doc.fieldType("binary"), OType.BINARY);
    assertEquals(doc.fieldType("link"), OType.LINK);
  }

  @Test
  public void testKeepSchemafullFieldTypeSerialization() throws Exception {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:" + ODocumentTest.class.getSimpleName());
    db.create();
    try {
      OClass clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.createProperty("integer", OType.INTEGER);
      clazz.createProperty("link", OType.LINK);
      clazz.createProperty("string", OType.STRING);
      clazz.createProperty("binary", OType.BINARY);
      ODocument doc = new ODocument(clazz);
      doc.field("integer", 10);
      doc.field("link", new ORecordId(1, 2));
      doc.field("string", "string");
      doc.field("binary", new byte[] { 30 });

      // the types are from the schema.
      assertEquals(doc.fieldType("integer"), OType.INTEGER);
      assertEquals(doc.fieldType("link"), OType.LINK);
      assertEquals(doc.fieldType("string"), OType.STRING);
      assertEquals(doc.fieldType("binary"), OType.BINARY);
      ORecordSerializer ser = ODatabaseDocumentTx.getDefaultSerializer();
      byte[] bytes = ser.toStream(doc, false);
      doc = new ODocument();
      ser.fromStream(bytes, doc, null);
      assertEquals(doc.fieldType("integer"), OType.INTEGER);
      assertEquals(doc.fieldType("string"), OType.STRING);
      assertEquals(doc.fieldType("binary"), OType.BINARY);
      assertEquals(doc.fieldType("link"), OType.LINK);
    } finally {
      db.drop();
    }
  }

  @Test
  public void testChangeTypeOnValueSet() throws Exception {
    ODocument doc = new ODocument();
    doc.field("link", new ORecordId(1, 2));
    ORecordSerializer ser = ODatabaseDocumentTx.getDefaultSerializer();
    byte[] bytes = ser.toStream(doc, false);
    doc = new ODocument();
    ser.fromStream(bytes, doc, null);
    assertEquals(doc.fieldType("link"), OType.LINK);
    doc.field("link", new ORidBag());
    assertNotEquals(doc.fieldType("link"), OType.LINK);
  }

  @Test
  public void testRemovingReadonlyField() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:" + ODocumentTest.class.getSimpleName());
    db.create();
    try {

      OSchema schema = db.getMetadata().getSchema();
      OClass classA = schema.createClass("TestRemovingField2");
      classA.createProperty("name", OType.STRING);
      OProperty property = classA.createProperty("property", OType.STRING);
      property.setReadonly(true);

      ODocument doc = new ODocument(classA);
      doc.field("name", "My Name");
      doc.field("property", "value1");
      doc.save();

      doc.field("name", "My Name 2");
      doc.field("property", "value2");
      doc.undo(); // we decided undo everything
      doc.field("name", "My Name 3"); // change something
      doc.save();
      doc.field("name", "My Name 4");
      doc.field("property", "value4");
      doc.undo("property");// we decided undo readonly field
      doc.save();
    } finally {
      db.drop();
    }
  }

  @Test
  public void testSetFieldAtListIndex() {
    ODocument doc = new ODocument();

    Map<String, Object> data = new HashMap<String, Object>();

    List<Object> parentArray = new ArrayList<Object>();
    parentArray.add(1);
    parentArray.add(2);
    parentArray.add(3);

    Map<String, Object> object4 = new HashMap<String, Object>();
    object4.put("prop", "A");
    parentArray.add(object4);

    data.put("array", parentArray);

    doc.field("data", data);

    assertEquals(doc.field("data.array[3].prop"), "A");
    doc.field("data.array[3].prop", "B");

    assertEquals(doc.field("data.array[3].prop"), "B");

    assertEquals(doc.field("data.array[0]"), 1);
    doc.field("data.array[0]", 5);

    assertEquals(doc.field("data.array[0]"), 5);
  }

  @Test
  public void testUndo() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:" + ODocumentTest.class.getSimpleName());
    db.create();
    try {

      OSchema schema = db.getMetadata().getSchema();
      OClass classA = schema.createClass("TestUndo");
      classA.createProperty("name", OType.STRING);
      classA.createProperty("property", OType.STRING);

      ODocument doc = new ODocument(classA);
      doc.field("name", "My Name");
      doc.field("property", "value1");
      doc.save();
      assertEquals(doc.field("name"), "My Name");
      assertEquals(doc.field("property"), "value1");
      doc.undo();
      assertEquals(doc.field("name"), "My Name");
      assertEquals(doc.field("property"), "value1");
      doc.field("name", "My Name 2");
      doc.field("property", "value2");
      doc.undo();
      doc.field("name", "My Name 3");
      assertEquals(doc.field("name"), "My Name 3");
      assertEquals(doc.field("property"), "value1");
      doc.save();
      doc.field("name", "My Name 4");
      doc.field("property", "value4");
      doc.undo("property");
      assertEquals(doc.field("name"), "My Name 4");
      assertEquals(doc.field("property"), "value1");
      doc.save();
      doc.undo("property");
      assertEquals(doc.field("name"), "My Name 4");
      assertEquals(doc.field("property"), "value1");
      doc.undo();
      assertEquals(doc.field("name"), "My Name 4");
      assertEquals(doc.field("property"), "value1");
    } finally {
      db.drop();
    }
  }

  @Test
  public void testMergeNull() {
    ODocument dest = new ODocument();

    ODocument source = new ODocument();
    source.field("key", "value");
    source.field("somenull", (Object) null);

    dest.merge(source, true, false);

    assertEquals(dest.field("key"), "value");

    assertTrue(dest.containsField("somenull"));

  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFailNestedSetNull() {
    ODocument doc = new ODocument();
    doc.field("test.nested", "value");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFailNullMapKey() {
    ODocument doc = new ODocument();
    Map<String, String> map = new HashMap<String, String>();
    map.put(null, "dd");
    doc.field("testMap", map);
    doc.convertAllMultiValuesToTrackedVersions();
  }

}
