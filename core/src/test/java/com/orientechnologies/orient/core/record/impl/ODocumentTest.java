package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.delta.ODocumentDelta;
import com.orientechnologies.orient.core.delta.ODocumentDeltaSerializer;
import com.orientechnologies.orient.core.delta.ODocumentDeltaSerializerI;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class ODocumentTest {
  private static final String dbName                    = ODocumentTest.class.getSimpleName();
  private static final String defaultDbAdminCredentials = "admin";

  @Test
  public void testCopyToCopiesEmptyFieldsTypesAndOwners() throws Exception {
    ODocument doc1 = new ODocument();

    ODocument doc2 = new ODocument().field("integer2", 123).field("string", "OrientDB").field("a", 123.3)

        .setFieldType("integer", OType.INTEGER).setFieldType("string", OType.STRING).setFieldType("binary", OType.BINARY);
    ODocumentInternal.addOwner(doc2, new ODocument());

    assertEquals(doc2.<Object>field("integer2"), 123);
    assertEquals(doc2.field("string"), "OrientDB");
    //    assertEquals(doc2.field("a"), 123.3);

    Assertions.assertThat(doc2.<Double>field("a")).isEqualTo(123.3d);
    assertEquals(doc2.fieldType("integer"), OType.INTEGER);
    assertEquals(doc2.fieldType("string"), OType.STRING);
    assertEquals(doc2.fieldType("binary"), OType.BINARY);

    assertNotNull(doc2.getOwner());

    doc1.copyTo(doc2);

    assertEquals(doc2.<Object>field("integer2"), null);
    assertEquals(doc2.<Object>field("string"), null);
    assertEquals(doc2.<Object>field("a"), null);

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
    byte[] bytes = ser.toStream(doc);
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
    byte[] bytes = ser.toStream(doc);
    doc = new ODocument();
    ser.fromStream(bytes, doc, null);
    assertEquals(doc.fieldType("integer"), OType.INTEGER);
    assertEquals(doc.fieldType("string"), OType.STRING);
    assertEquals(doc.fieldType("binary"), OType.BINARY);
    assertEquals(doc.fieldType("link"), OType.LINK);
  }

  @Test
  public void testKeepSchemafullFieldTypeSerialization() throws Exception {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

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
      byte[] bytes = ser.toStream(doc);
      doc = new ODocument();
      ser.fromStream(bytes, doc, null);
      assertEquals(doc.fieldType("integer"), OType.INTEGER);
      assertEquals(doc.fieldType("string"), OType.STRING);
      assertEquals(doc.fieldType("binary"), OType.BINARY);
      assertEquals(doc.fieldType("link"), OType.LINK);
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testChangeTypeOnValueSet() throws Exception {
    ODocument doc = new ODocument();
    doc.field("link", new ORecordId(1, 2));
    ORecordSerializer ser = ODatabaseDocumentTx.getDefaultSerializer();
    byte[] bytes = ser.toStream(doc);
    doc = new ODocument();
    ser.fromStream(bytes, doc, null);
    assertEquals(doc.fieldType("link"), OType.LINK);
    doc.field("link", new ORidBag());
    assertNotEquals(doc.fieldType("link"), OType.LINK);
  }

  @Test
  public void testRemovingReadonlyField() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

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
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
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

    assertEquals(doc.<Object>field("data.array[0]"), 1);
    doc.field("data.array[0]", 5);

    assertEquals(doc.<Object>field("data.array[0]"), 5);
  }

  @Test
  public void testUndo() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

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
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
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

  @Test(expected = IllegalArgumentException.class)
  public void testFailNestedSetNull() {
    ODocument doc = new ODocument();
    doc.field("test.nested", "value");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailNullMapKey() {
    ODocument doc = new ODocument();
    Map<String, String> map = new HashMap<String, String>();
    map.put(null, "dd");
    doc.field("testMap", map);
    doc.convertAllMultiValuesToTrackedVersions();
  }

  @Test
  public void testGetSetProperty() {
    ODocument doc = new ODocument();
    Map<String, String> map = new HashMap<String, String>();
    map.put("foo", "valueInTheMap");
    doc.field("theMap", map);
    doc.setProperty("theMap.foo", "bar");

    assertEquals(doc.getProperty("theMap"), map);
    assertEquals(doc.getProperty("theMap.foo"), "bar");
    assertEquals(doc.eval("theMap.foo"), "valueInTheMap");

//    doc.setProperty("", "foo");
//    assertEquals(doc.getProperty(""), "foo");

    doc.setProperty(",", "comma");
    assertEquals(doc.getProperty(","), "comma");

    doc.setProperty(",.,/;:'\"", "strange");
    assertEquals(doc.getProperty(",.,/;:'\""), "strange");

    doc.setProperty("   ", "spaces");
    assertEquals(doc.getProperty("   "), "spaces");
  }

  @Test
  public void testNoDirtySameBytes() {
    ODocument doc = new ODocument();
    byte[] bytes = new byte[] { 0, 1, 2, 3, 4, 5 };
    doc.field("bytes", bytes);
    ODocumentInternal.clearTrackData(doc);
    ORecordInternal.unsetDirty(doc);
    assertFalse(doc.isDirty());
    assertNull(doc.getOriginalValue("bytes"));
    doc.field("bytes", bytes.clone());
    assertFalse(doc.isDirty());
    assertNull(doc.getOriginalValue("bytes"));
  }

  @Test
  public void testGetFromOriginalSimpleDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      String constantFieldName = "constantField";
      String originalValue = "orValue";
      String testValue = "testValue";
      String removeField = "removeField";

      doc.field(fieldName, originalValue);
      doc.field(constantFieldName, "someValue");
      doc.field(removeField, "removeVal");

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      doc.field(fieldName, testValue);
      doc.removeField(removeField);
      ODocumentDelta dc = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(dc);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(dc, dcCopy);

      ODocumentDelta updatePart = dc.field("u").getValue();

      originalDoc.mergeUpdateDelta(updatePart);
      assertEquals(testValue, originalDoc.field(fieldName));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testGetFromOriginalSimpleDeltaWithCopy() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      String constantFieldName = "constantField";
      String originalValue = "orValue";
      String testValue = "testValue";
      String removeField = "removeField";

      doc.field(fieldName, originalValue);
      doc.field(constantFieldName, "someValue");
      doc.field(removeField, "removeVal");

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      doc.field(fieldName, testValue);
      doc.removeField(removeField);
      ODocumentDelta dc = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(dc);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(dc, dcCopy);

      ODocumentDelta updatePart = dc.field("u").getValue();

      originalDoc.mergeUpdateDelta(updatePart);
      assertEquals(testValue, originalDoc.field(fieldName));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testGetFromNestedDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      ODocument nestedDoc = new ODocument(claz);
      String fieldName = "testField";
      String constantFieldName = "constantField";
      String originalValue = "orValue";
      String testValue = "testValue";
      String nestedDocField = "nestedField";

      nestedDoc.field(fieldName, originalValue);
      nestedDoc.field(constantFieldName, "someValue1");

      doc.field(constantFieldName, "someValue2");
      doc.field(nestedDocField, nestedDoc);

      ODocument originalDoc = new ODocument();
      originalDoc.field(constantFieldName, "someValue2");
      originalDoc.field(nestedDocField, nestedDoc);

      doc = db.save(doc);

      nestedDoc = doc.field(nestedDocField);
      nestedDoc.field(fieldName, testValue);

      doc.field(nestedDocField, nestedDoc);

      ODocumentDelta dc = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(dc);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(dc, dcCopy);

      dc = dc.field("u").getValue();

      originalDoc.mergeUpdateDelta(dc);
      nestedDoc = originalDoc.field(nestedDocField);
      assertEquals(nestedDoc.field(fieldName), testValue);
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testGetFromNestedDeltaWithCopy() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      ODocument nestedDoc = new ODocument(claz);
      String fieldName = "testField";
      String constantFieldName = "constantField";
      String originalValue = "orValue";
      String testValue = "testValue";
      String nestedDocField = "nestedField";

      nestedDoc.field(fieldName, originalValue);
      nestedDoc.field(constantFieldName, "someValue1");

      doc.field(constantFieldName, "someValue2");
      doc.field(nestedDocField, nestedDoc);

      ODocument originalDoc = new ODocument();
      originalDoc.field(constantFieldName, "someValue2");
      originalDoc.field(nestedDocField, nestedDoc);

      doc = db.save(doc);

      nestedDoc = ((ODocument) doc.field(nestedDocField)).copy();
      ORecordInternal.setIdentity(nestedDoc, ORecordId.EMPTY_RECORD_ID);
      nestedDoc.field(fieldName, testValue);
      doc.field(nestedDocField, nestedDoc);

      ODocumentDelta dc = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(dc);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(dc, dcCopy);

      dc = dc.field("u").getValue();

      doc.mergeUpdateDelta(dc);
      nestedDoc = doc.field(nestedDocField);
      assertEquals(nestedDoc.field(fieldName), testValue);
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      ODocument originalDoc = new ODocument(claz);

      String fieldName = "testField";
      List<String> originalValue = new ArrayList<>();
      originalValue.add("one");
      originalValue.add("two");
      List<String> copyList = new ArrayList<>(originalValue);

      doc.field(fieldName, originalValue);
      originalDoc.field(fieldName, copyList);

      doc = db.save(doc);

      List<String> newArray = doc.field(fieldName);
      newArray.set(1, "three");

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List checkList = originalDoc.field(fieldName);
      assertEquals("three", checkList.get(1));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListDeltaWithCopy() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      ODocument originalDoc = new ODocument(claz);

      String fieldName = "testField";
      List<String> originalValue = new ArrayList<>();
      originalValue.add("one");
      originalValue.add("two");
      List<String> copyList = new ArrayList<>(originalValue);

      doc.field(fieldName, originalValue);
      originalDoc.field(fieldName, copyList);

      doc = db.save(doc);

      List<String> newArray = new ArrayList<>(originalValue);
      newArray.set(1, "three");
      doc.field(fieldName, newArray);

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List checkList = originalDoc.field(fieldName);
      assertEquals("three", checkList.get(1));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListOfListsDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      List<List<String>> originalValue = new ArrayList<>();
      List<List<String>> copyValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        List<String> containedList = new ArrayList<>();
        containedList.add("one");
        containedList.add("two");

        List<String> copyContainedList = new ArrayList<>(containedList);
        originalValue.add(containedList);
        copyValue.add(copyContainedList);
      }

      doc.field(fieldName, originalValue);
      ODocument originalDoc = new ODocument(claz);
      originalDoc.field(fieldName, copyValue);

      doc = db.save(doc);

      List<String> newArray = (List<String>) ((List) doc.field(fieldName)).get(0);
      newArray.set(1, "three");

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List<List> checkList = originalDoc.field(fieldName);
      assertEquals("three", checkList.get(0).get(1));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListOfListsDeltaWithCopy() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      List<List<String>> originalValue = new ArrayList<>();
      List<List<String>> copyValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        List<String> containedList = new ArrayList<>();
        containedList.add("one");
        containedList.add("two");

        List<String> copyContainedList = new ArrayList<>(containedList);
        originalValue.add(containedList);
        copyValue.add(copyContainedList);
      }

      doc.field(fieldName, originalValue);
      ODocument originalDoc = new ODocument(claz);
      originalDoc.field(fieldName, copyValue);

      doc = db.save(doc);

      List<String> newArray = new ArrayList<>(originalValue.get(0));
      newArray.set(1, "three");
      originalValue = new ArrayList<>(originalValue);
      originalValue.set(0, newArray);
      doc.field(fieldName, originalValue);

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List<List> checkList = originalDoc.field(fieldName);
      assertEquals("three", checkList.get(0).get(1));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListOfDocsDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      String fieldName = "testField";

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      ODocument originalDoc = new ODocument(claz);

      String constantField = "constField";
      String constValue = "ConstValue";
      String variableField = "varField";
      List<ODocument> originalValue = new ArrayList<>();
      List<ODocument> copyValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        ODocument containedDoc = new ODocument();
        containedDoc.field(constantField, constValue);
        containedDoc.field(variableField, "one" + i);
        originalValue.add(containedDoc);
        copyValue.add(containedDoc.copy());
      }

      doc.field(fieldName, originalValue);
      originalDoc.field(fieldName, copyValue);

      doc = db.save(doc);

      ODocument testDoc = (ODocument) ((List) doc.getProperty(fieldName)).get(1);
      testDoc.field(variableField, "two");

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List<ODocument> checkList = originalDoc.field(fieldName);
      ODocument checkDoc = checkList.get(1);
      assertEquals(checkDoc.field(constantField), constValue);
      assertEquals(checkDoc.field(variableField), "two");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListOfDocsDeltaWithCopy() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      String fieldName = "testField";

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      ODocument originalDoc = new ODocument(claz);

      String constantField = "constField";
      String constValue = "ConstValue";
      String variableField = "varField";
      List<ODocument> originalValue = new ArrayList<>();
      List<ODocument> copyValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        ODocument containedDoc = new ODocument();
        containedDoc.field(constantField, constValue);
        containedDoc.field(variableField, "one" + i);
        originalValue.add(containedDoc);
        copyValue.add(containedDoc.copy());
      }

      doc.field(fieldName, originalValue);
      originalDoc.field(fieldName, copyValue);

      doc = db.save(doc);

      ODocument testDoc = originalValue.get(1).copy();
      ORecordInternal.setIdentity(testDoc, ORecordId.EMPTY_RECORD_ID);
      testDoc.field(variableField, "two");
      originalValue = new ArrayList<>(originalValue);
      originalValue.set(1, testDoc);
      doc.field(fieldName, originalValue);
      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List<ODocument> checkList = originalDoc.field(fieldName);
      ODocument checkDoc = checkList.get(1);
      assertEquals(checkDoc.field(constantField), constValue);
      assertEquals(checkDoc.field(variableField), "two");
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListOfListsOfDocumentDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      String constantField = "constField";
      String constValue = "ConstValue";
      String variableField = "varField";

      ODocument doc = new ODocument(claz);
      ODocument originalDoc = new ODocument(claz);

      String fieldName = "testField";
      List<List<ODocument>> originalValue = new ArrayList<>();
      List<List<ODocument>> copyValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        List<ODocument> containedList = new ArrayList<>();
        List<ODocument> copyContainedList = new ArrayList<>();
        ODocument d1 = new ODocument();
        d1.field(constantField, constValue);
        d1.field(variableField, "one");
        ODocument d2 = new ODocument();
        d2.field(constantField, constValue);
        containedList.add(d1);
        containedList.add(d2);
        copyContainedList.add(d1.copy());
        copyContainedList.add(d2.copy());
        originalValue.add(containedList);
        copyValue.add(copyContainedList);
      }

      doc.field(fieldName, originalValue);
      originalDoc.field(fieldName, copyValue);

      doc = db.save(doc);

      ODocument d1 = originalValue.get(0).get(0);
      d1.field(variableField, "two");

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List<List<ODocument>> checkList = originalDoc.field(fieldName);
      assertEquals("two", checkList.get(0).get(0).field(variableField));
    } catch (Exception e) {
      e.printStackTrace();
      assertNotNull(null);
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListOfListsOfDocumentDeltaWithCopy() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      String constantField = "constField";
      String constValue = "ConstValue";
      String variableField = "varField";

      ODocument doc = new ODocument(claz);
      ODocument originalDoc = new ODocument(claz);

      String fieldName = "testField";
      List<List<ODocument>> originalValue = new ArrayList<>();
      List<List<ODocument>> copyValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        List<ODocument> containedList = new ArrayList<>();
        List<ODocument> copyContainedList = new ArrayList<>();
        ODocument d1 = new ODocument();
        d1.field(constantField, constValue);
        d1.field(variableField, "one");
        ODocument d2 = new ODocument();
        d2.field(constantField, constValue);
        containedList.add(d1);
        containedList.add(d2);
        copyContainedList.add(d1.copy());
        copyContainedList.add(d2.copy());
        originalValue.add(containedList);
        copyValue.add(copyContainedList);
      }

      doc.field(fieldName, originalValue);
      originalDoc.field(fieldName, copyValue);

      doc = db.save(doc);

      List<ODocument> newArray = new ArrayList<>(originalValue.get(0));
      ODocument d1 = newArray.get(0).copy();
      ORecordInternal.setIdentity(d1, ORecordId.EMPTY_RECORD_ID);
      d1.field(variableField, "two");
      newArray.set(0, d1);
      originalValue = new ArrayList<>(originalValue);
      originalValue.set(0, newArray);
      doc.field(fieldName, originalValue);

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List<List<ODocument>> checkList = originalDoc.field(fieldName);
      assertEquals("two", checkList.get(0).get(0).field(variableField));
    } catch (Exception e) {
      e.printStackTrace();
      assertNotNull(null);
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListOfListsOfListDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      List<List<List<String>>> originalValue = new ArrayList<>();
      List<List<List<String>>> copyValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        List<List<String>> containedList = new ArrayList<>();
        List<List<String>> copyConatinedList = new ArrayList<>();
        for (int j = 0; j < 2; j++) {
          List<String> innerList = new ArrayList<>();
          innerList.add("el1" + j + i);
          innerList.add("el2" + j + i);
          containedList.add(innerList);

          List<String> copyInnerList = new ArrayList<>(innerList);
          copyConatinedList.add(copyInnerList);
        }
        originalValue.add(containedList);
        copyValue.add(copyConatinedList);
      }

      doc.field(fieldName, originalValue);
      ODocument originalDoc = new ODocument(claz);
      originalDoc.field(fieldName, copyValue);

      doc = db.save(doc);

      List<String> innerList = (List<String>) ((List) ((List) ((List) doc.field(fieldName)).get(0)).get(0));
      innerList.set(0, "changed");

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List<List<List<String>>> checkList = originalDoc.field(fieldName);
      assertEquals("changed", checkList.get(0).get(0).get(0));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListOfListsOfListDeltaWithCopy() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      List<List<List<String>>> originalValue = new ArrayList<>();
      List<List<List<String>>> copyValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        List<List<String>> containedList = new ArrayList<>();
        List<List<String>> copyConatinedList = new ArrayList<>();
        for (int j = 0; j < 2; j++) {
          List<String> innerList = new ArrayList<>();
          innerList.add("el1" + j + i);
          innerList.add("el2" + j + i);
          containedList.add(innerList);

          List<String> copyInnerList = new ArrayList<>(innerList);
          copyConatinedList.add(copyInnerList);
        }
        originalValue.add(containedList);
        copyValue.add(copyConatinedList);
      }

      doc.field(fieldName, originalValue);
      ODocument originalDoc = new ODocument(claz);
      originalDoc.field(fieldName, copyValue);

      doc = db.save(doc);

      List<String> innerList = new ArrayList<>();
      innerList.add("changed");
      innerList.add("el200");
      List<List> firstLevelList = doc.field(fieldName);
      List<List<List<String>>> newFirstLevelList = new ArrayList<>();
      List secondLevelList = new ArrayList(firstLevelList.get(0));
      secondLevelList.set(0, innerList);
      newFirstLevelList.add(secondLevelList);
      newFirstLevelList.add(firstLevelList.get(1));
      doc.field(fieldName, newFirstLevelList);

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List<List<List<String>>> checkList = originalDoc.field(fieldName);
      assertEquals("changed", checkList.get(0).get(0).get(0));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListOfDocsWithList() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      String fieldName = "testField";

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      ODocument originalDoc = new ODocument(claz);

      String constantField = "constField";
      String constValue = "ConstValue";
      String variableField = "varField";
      List<ODocument> originalValue = new ArrayList<>();
      List<ODocument> copyValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        ODocument containedDoc = new ODocument();
        containedDoc.field(constantField, constValue);
        List<String> listField = new ArrayList<>();
        for (int j = 0; j < 2; j++) {
          listField.add("Some" + j);
        }
        containedDoc.field(variableField, listField);
        originalValue.add(containedDoc);

        List<String> copyListField = new ArrayList<>(listField);
        ODocument copyContainedDoc = new ODocument();
        copyContainedDoc.field(variableField, copyListField);
        copyValue.add(copyContainedDoc);
      }

      doc.field(fieldName, originalValue);
      originalDoc.field(fieldName, copyValue);

      doc = db.save(doc);

      ODocument testDoc = (ODocument) ((List) doc.field(fieldName)).get(1);
      List<String> currentList = testDoc.field(variableField);
      currentList.set(0, "changed");

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List<ODocument> checkList = originalDoc.field(fieldName);
      ODocument checkDoc = checkList.get(1);
      List<String> checkInnerList = checkDoc.field(variableField);
      assertEquals("changed", checkInnerList.get(0));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListOfDocsWithListWithCopy() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      String fieldName = "testField";

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      ODocument originalDoc = new ODocument(claz);

      String constantField = "constField";
      String constValue = "ConstValue";
      String variableField = "varField";
      List<ODocument> originalValue = new ArrayList<>();
      List<ODocument> copyValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        ODocument containedDoc = new ODocument();
        containedDoc.field(constantField, constValue);
        List<String> listField = new ArrayList<>();
        for (int j = 0; j < 2; j++) {
          listField.add("Some" + j);
        }
        containedDoc.field(variableField, listField);
        originalValue.add(containedDoc);

        List<String> copyListField = new ArrayList<>(listField);
        ODocument copyContainedDoc = new ODocument();
        copyContainedDoc.field(variableField, copyListField);
        copyValue.add(copyContainedDoc);
      }

      doc.field(fieldName, originalValue);
      originalDoc.field(fieldName, copyValue);

      doc = db.save(doc);

      ODocument testDoc = originalValue.get(1).copy();
      ORecordInternal.setIdentity(testDoc, ORecordId.EMPTY_RECORD_ID);
      testDoc.field(constantField, constValue);
      List<String> currentList = testDoc.field(variableField);
      List<String> newList = new ArrayList<>(currentList);
      newList.set(0, "changed");
      testDoc.field(variableField, newList);
      originalValue.set(1, testDoc);
      doc.field(fieldName, originalValue);
      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List<ODocument> checkList = originalDoc.field(fieldName);
      ODocument checkDoc = checkList.get(1);
      List<String> checkInnerList = checkDoc.field(variableField);
      assertEquals("changed", checkInnerList.get(0));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListAddDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      ODocument originalDoc = new ODocument(claz);

      String fieldName = "testField";
      List<String> originalValue = new ArrayList<>();
      originalValue.add("one");
      originalValue.add("two");
      List<String> copyValue = new ArrayList<>(originalValue);

      doc.field(fieldName, originalValue);
      originalDoc.field(fieldName, copyValue);

      doc = db.save(doc);

      List<String> newArray = doc.field(fieldName);
      newArray.add("three");

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List checkList = originalDoc.field(fieldName);
      assertEquals(3, checkList.size());
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListAddDeltaWithCopy() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      ODocument originalDoc = new ODocument(claz);

      String fieldName = "testField";
      List<String> originalValue = new ArrayList<>();
      originalValue.add("one");
      originalValue.add("two");
      List<String> copyValue = new ArrayList<>(originalValue);

      doc.field(fieldName, originalValue);
      originalDoc.field(fieldName, copyValue);

      doc = db.save(doc);

      List<String> newArray = new ArrayList<>();
      newArray.add("one");
      newArray.add("two");
      newArray.add("three");
      doc.field(fieldName, newArray);

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List checkList = originalDoc.field(fieldName);
      assertEquals(3, checkList.size());
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListOfListAddDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      ODocument originalDoc = new ODocument(claz);

      String fieldName = "testField";
      List<List<String>> originalList = new ArrayList<>();
      List<List<String>> copyList = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        List<String> nestedList = new ArrayList<>();
        nestedList.add("one");
        nestedList.add("two");
        originalList.add(nestedList);

        List<String> copyNestedList = new ArrayList<>(nestedList);
        copyList.add(copyNestedList);
      }

      doc.field(fieldName, originalList);
      originalDoc.field(fieldName, copyList);

      doc = db.save(doc);

      List<String> newArray = (List<String>) ((List) doc.field(fieldName)).get(0);
      newArray.add("three");

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List<List<String>> rootList = originalDoc.field(fieldName);
      List<String> checkList = rootList.get(0);
      assertEquals(3, checkList.size());
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListOfListAddDeltaWithCopy() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      ODocument originalDoc = new ODocument(claz);

      String fieldName = "testField";
      List<List<String>> originalList = new ArrayList<>();
      List<List<String>> copyList = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        List<String> nestedList = new ArrayList<>();
        nestedList.add("one");
        nestedList.add("two");
        originalList.add(nestedList);

        List<String> copyNestedList = new ArrayList<>(nestedList);
        copyList.add(copyNestedList);
      }

      doc.field(fieldName, originalList);
      originalDoc.field(fieldName, copyList);

      doc = db.save(doc);

      List<String> newArray = new ArrayList<>();
      newArray.add("one");
      newArray.add("two");
      newArray.add("three");
      originalList = new ArrayList<>(originalList);
      originalList.set(0, newArray);
      doc.field(fieldName, originalList);

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List<List<String>> rootList = originalDoc.field(fieldName);
      List<String> checkList = rootList.get(0);
      assertEquals(3, checkList.size());
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListRemoveDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      ODocument originalDoc = new ODocument(claz);

      String fieldName = "testField";
      List<String> originalValue = new ArrayList<>();
      originalValue.add("one");
      originalValue.add("two");
      originalValue.add("three");
      List<String> copyValue = new ArrayList<>(originalValue);

      doc.field(fieldName, originalValue);
      originalDoc.field(fieldName, copyValue);

      doc = db.save(doc);

      List<String> newArray = (List<String>) doc.field(fieldName);
      newArray.remove(0);
      newArray.remove(0);

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List checkList = originalDoc.field(fieldName);
      assertEquals("three", checkList.get(0));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListRemoveDeltaWithCopy() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      ODocument originalDoc = new ODocument(claz);

      String fieldName = "testField";
      List<String> originalValue = new ArrayList<>();
      originalValue.add("one");
      originalValue.add("two");
      originalValue.add("three");
      List<String> copyValue = new ArrayList<>(originalValue);

      doc.field(fieldName, originalValue);
      originalDoc.field(fieldName, copyValue);

      doc = db.save(doc);

      List<String> newArray = new ArrayList<>();
      newArray.add("one");
      doc.field(fieldName, newArray);

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("u").getValue();

      originalDoc.mergeUpdateDelta(delta);
      List checkList = originalDoc.field(fieldName);
      assertEquals(1, checkList.size());
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testAddDocFieldDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      String constantFieldName = "constantField";
      String testValue = "testValue";

      doc.field(constantFieldName + "1", "someValue1");
      doc.field(constantFieldName, "someValue");

      doc = db.save(doc);
      ODocument originaDoc = doc.copy();

      doc.field(fieldName, testValue);
      ODocumentDelta dc = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(dc);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(dc, dcCopy);

      ODocumentDelta updatePart = dc.field("u").getValue();

      originaDoc.mergeUpdateDelta(updatePart);
      assertEquals(testValue, originaDoc.field(fieldName));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testRemoveDocFieldDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      String constantFieldName = "constantField";
      String testValue = "testValue";

      doc.field(fieldName, testValue);
      doc.field(constantFieldName, "someValue");

      doc = db.save(doc);
      ODocument originaDoc = doc.copy();

      doc.removeField(fieldName);
      ODocumentDelta dc = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(dc);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(dc, dcCopy);

      ODocumentDelta deletePart = dc.field("d").getValue();

      originaDoc.mergeDeleteDelta(deletePart);
      assertFalse(originaDoc.containsField(fieldName));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testRemoveNestedDocFieldDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      String nestedFieldName = "nested";

      OClass claz = db.createClassIfNotExist("TestClass");
      claz.createProperty(nestedFieldName, OType.EMBEDDED);

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      String constantFieldName = "constantField";
      String testValue = "testValue";

      doc.field(fieldName, testValue);
      doc.field(constantFieldName, "someValue");

      ODocument rootDoc = new ODocument(claz);
      rootDoc.field(nestedFieldName, doc);
      ODocument originalDoc = new ODocument(claz);
      originalDoc.field(nestedFieldName, doc.copy());

      rootDoc = db.save(rootDoc);

      doc.removeField(fieldName);

      ODocumentDelta dc = rootDoc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(dc);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(dc, dcCopy);

      ODocumentDelta deletePart = dc.field("d").getValue();

      originalDoc.mergeDeleteDelta(deletePart);
      ODocument nested = originalDoc.field(nestedFieldName);
      assertFalse(nested.containsField(fieldName));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testRemoveFieldListOfDocsDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      String fieldName = "testField";

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      ODocument originalDoc = new ODocument(claz);

      String constantField = "constField";
      String constValue = "ConstValue";
      String variableField = "varField";
      List<ODocument> originalValue = new ArrayList<>();
      List<ODocument> copyValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        ODocument containedDoc = new ODocument();
        containedDoc.field(constantField, constValue);
        containedDoc.field(variableField, "one" + i);
        originalValue.add(containedDoc);
        copyValue.add(containedDoc.copy());
      }

      doc.field(fieldName, originalValue);
      originalDoc.field(fieldName, copyValue);

      doc = db.save(doc);

      ODocument testDoc = (ODocument) ((List) doc.field(fieldName)).get(1);
      testDoc.removeField(variableField);

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("d").getValue();

      originalDoc.mergeDeleteDelta(delta);
      List<ODocument> checkList = originalDoc.field(fieldName);
      ODocument checkDoc = checkList.get(1);
      assertEquals(checkDoc.field(constantField), constValue);
      assertFalse(checkDoc.containsField(variableField));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListOfDocsRemoveFieldWithCopy() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      String fieldName = "testField";

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      ODocument originalDoc = new ODocument(claz);

      String constantField = "constField";
      String constValue = "ConstValue";
      String variableField = "varField";
      List<ODocument> originalValue = new ArrayList<>();
      List<ODocument> copyValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        ODocument containedDoc = new ODocument();
        containedDoc.field(constantField, constValue);
        containedDoc.field(variableField, "one" + i);
        originalValue.add(containedDoc);
        copyValue.add(containedDoc.copy());
      }

      doc.field(fieldName, originalValue);
      originalDoc.field(fieldName, copyValue);

      doc = db.save(doc);

      ODocument testDoc = originalValue.get(1).copy();
      ORecordInternal.setIdentity(testDoc, ORecordId.EMPTY_RECORD_ID);
      testDoc.removeField(variableField);
      originalValue = new ArrayList<>(originalValue);
      originalValue.set(1, testDoc);
      doc.field(fieldName, originalValue);

      ODocumentDelta delta = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(delta);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(delta, dcCopy);

      delta = delta.field("d").getValue();

      originalDoc.mergeDeleteDelta(delta);
      List<ODocument> checkList = originalDoc.field(fieldName);
      ODocument checkDoc = checkList.get(1);
      assertEquals(checkDoc.field(constantField), constValue);
      assertFalse(checkDoc.containsField(variableField));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testUpdateEmbeddedMapDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      Map<String, String> mapValue = new HashMap<>();
      mapValue.put("first", "one");
      mapValue.put("second", "two");
      Map<String, String> copyMap = new HashMap<>(mapValue);

      doc.field(fieldName, mapValue, OType.EMBEDDEDMAP);
      ODocument originalDoc = new ODocument(claz);
      originalDoc.field(fieldName, copyMap, OType.EMBEDDEDMAP);

      doc = db.save(doc);

      Map<String, String> containedMap = doc.field(fieldName);
      containedMap.put("first", "changed");
      ODocumentDelta dc = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(dc);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(dc, dcCopy);

      ODocumentDelta updatePart = dc.field("u").getValue();

      originalDoc.mergeUpdateDelta(updatePart);
      containedMap = originalDoc.field(fieldName);
      assertEquals("changed", containedMap.get("first"));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testUpdateListOfEmbeddedMapDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      List<Map<String, String>> originalValue = new ArrayList<>();
      List<Map<String, String>> copyValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        Map<String, String> mapValue = new HashMap<>();
        mapValue.put("first", "one");
        mapValue.put("second", "two");
        Map<String, String> copyMap = new HashMap<>(mapValue);

        originalValue.add(mapValue);
        copyValue.add(copyMap);
      }

      doc.field(fieldName, originalValue, OType.EMBEDDEDLIST);
      ODocument originalDoc = new ODocument(claz);
      originalDoc.field(fieldName, copyValue);

      doc = db.save(doc);

      Map<String, String> containedMap = (Map<String, String>) ((List) doc.field(fieldName)).get(0);
      containedMap.put("first", "changed");
      ODocumentDelta dc = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(dc);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(dc, dcCopy);

      ODocumentDelta updatePart = dc.field("u").getValue();

      originalDoc.mergeUpdateDelta(updatePart);
      containedMap = (Map<String, String>) ((List) originalDoc.field(fieldName)).get(0);
      assertEquals("changed", containedMap.get("first"));
      containedMap = (Map<String, String>) ((List) originalDoc.field(fieldName)).get(1);
      assertEquals("one", containedMap.get("first"));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testUpdateDocInMapDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      Map<String, ODocument> mapValue = new HashMap<>();
      ODocument d1 = new ODocument();
      d1.field("f1", "v1");
      mapValue.put("first", d1);
      ODocument d2 = new ODocument();
      d2.field("f2", "v2");
      mapValue.put("second", d2);
      Map<String, ODocument> copyMap = new HashMap<>();
      copyMap.put("first", d1.copy());
      copyMap.put("second", d2.copy());

      doc.field(fieldName, mapValue, OType.EMBEDDEDMAP);
      ODocument originalDoc = new ODocument(claz);
      originalDoc.field(fieldName, copyMap, OType.EMBEDDEDMAP);

      doc = db.save(doc);

      Map<String, ODocument> containedMap = doc.field(fieldName);
      ODocument changeDoc = containedMap.get("first");
      changeDoc.field("f1", "changed");

      ODocumentDelta dc = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(dc);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(dc, dcCopy);

      ODocumentDelta updatePart = dc.field("u").getValue();

      originalDoc.mergeUpdateDelta(updatePart);
      containedMap = originalDoc.field(fieldName);
      ODocument containedDoc = containedMap.get("first");
      assertEquals("changed", containedDoc.field("f1"));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testListOfMapsUpdateDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      List<Map> originalList = new ArrayList<>();
      List<Map> copyList = new ArrayList<>();

      Map<String, String> mapValue1 = new HashMap<>();
      mapValue1.put("first", "one");
      mapValue1.put("second", "two");
      originalList.add(mapValue1);
      Map<String, String> mapValue1Copy = new HashMap<>(mapValue1);
      copyList.add(mapValue1Copy);

      Map<String, String> mapValue2 = new HashMap<>();
      mapValue2.put("third", "three");
      mapValue2.put("forth", "four");
      originalList.add(mapValue2);
      Map<String, String> mapValue2Copy = new HashMap<>(mapValue2);
      copyList.add(mapValue2Copy);

      doc.field(fieldName, originalList);
      ODocument originalDoc = new ODocument(claz);
      originalDoc.field(fieldName, copyList);

      doc = db.save(doc);

      Map<String, String> containedMap = (Map<String, String>) ((List) doc.field(fieldName)).get(0);
      containedMap.put("first", "changed");

      ODocumentDelta dc = doc.getDeltaFromOriginal();

      //test serialization/deserialization
      ODocumentDeltaSerializerI ddSer = ODocumentDeltaSerializer.getActiveSerializer();
      byte[] stream = ddSer.toStream(dc);
      BytesContainer bytes = new BytesContainer(stream);
      ODocumentDelta dcCopy = ddSer.fromStream(bytes);
      assertEquals(dc, dcCopy);

      ODocumentDelta updatePart = dc.field("u").getValue();

      originalDoc.mergeUpdateDelta(updatePart);
      containedMap = (Map<String, String>) ((List) originalDoc.field(fieldName)).get(0);
      assertEquals("changed", containedMap.get("first"));
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testRidbagsUpdateDeltaAddWithCopy() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";

      ODocument first = new ODocument(claz);
      first = db.save(first);
      ODocument second = new ODocument(claz);
      second = db.save(second);

      ORidBag ridBag = new ORidBag();
      ridBag.add(first);
      ridBag.add(second);
      doc.field(fieldName, ridBag, OType.LINKBAG);
      doc = db.save(doc);

      ODocument originalDoc = new ODocument(claz);
      ORidBag ridBagCopy = new ORidBag();
      ridBagCopy.add(first);
      ridBagCopy.add(second);
      originalDoc.field(fieldName, ridBagCopy, OType.LINKBAG);

      ODocument third = new ODocument(claz);
      third = db.save(third);
      ridBag = new ORidBag();
      ridBag.add(first);
      ridBag.add(second);
      ridBag.add(third);
      doc.field(fieldName, ridBag, OType.LINKBAG);

      ODocumentDelta dc = doc.getDeltaFromOriginal();
      dc = dc.field("u").getValue();
      originalDoc.mergeUpdateDelta(dc);
      ORidBag mergedRidbag = originalDoc.field(fieldName);
      assertEquals(ridBag, mergedRidbag);
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testRidbagsUpdateDeltaRemoveWithCopy() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";

      ODocument first = new ODocument(claz);
      first = db.save(first);
      ODocument second = new ODocument(claz);
      second = db.save(second);
      ODocument third = new ODocument(claz);
      third = db.save(third);

      ORidBag ridBag = new ORidBag();
      ridBag.add(first);
      ridBag.add(second);
      ridBag.add(third);
      doc.field(fieldName, ridBag, OType.LINKBAG);
      doc = db.save(doc);

      ODocument originalDoc = new ODocument(claz);
      ORidBag ridBagCopy = new ORidBag();
      ridBagCopy.add(first);
      ridBagCopy.add(second);
      ridBagCopy.add(third);
      originalDoc.field(fieldName, ridBagCopy, OType.LINKBAG);

      ridBag = new ORidBag();
      ridBag.add(first);
      ridBag.add(second);
      doc.field(fieldName, ridBag, OType.LINKBAG);

      ODocumentDelta dc = doc.getDeltaFromOriginal();
      dc = dc.field("u").getValue();
      originalDoc.mergeUpdateDelta(dc);
      ORidBag mergedRidbag = originalDoc.field(fieldName);
      assertEquals(ridBag, mergedRidbag);
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testRidbagsUpdateDeltaAdd() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";

      ODocument first = new ODocument(claz);
      first = db.save(first);
      ODocument second = new ODocument(claz);
      second = db.save(second);

      ORidBag ridBag = new ORidBag();
      ridBag.add(first);
      ridBag.add(second);
      doc.field(fieldName, ridBag, OType.LINKBAG);
      doc = db.save(doc);

      ODocument originalDoc = new ODocument(claz);
      ORidBag ridBagCopy = new ORidBag();
      ridBagCopy.add(first);
      ridBagCopy.add(second);
      originalDoc.field(fieldName, ridBagCopy, OType.LINKBAG);

      ODocument third = new ODocument(claz);
      third = db.save(third);
      ridBag.add(third);

      ODocumentDelta dc = doc.getDeltaFromOriginal();
      dc = dc.field("u").getValue();
      originalDoc.mergeUpdateDelta(dc);
      ORidBag mergedRidbag = originalDoc.field(fieldName);
      assertEquals(ridBag, mergedRidbag);
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testRidbagsUpdateDeltaRemove() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";

      ODocument first = new ODocument(claz);
      first = db.save(first);
      ODocument second = new ODocument(claz);
      second = db.save(second);
      ODocument third = new ODocument(claz);
      third = db.save(third);

      ORidBag ridBag = new ORidBag();
      ridBag.add(first);
      ridBag.add(second);
      ridBag.add(third);
      doc.field(fieldName, ridBag, OType.LINKBAG);
      doc = db.save(doc);

      ODocument originalDoc = new ODocument(claz);
      ORidBag ridBagCopy = new ORidBag();
      ridBagCopy.add(first);
      ridBagCopy.add(second);
      ridBagCopy.add(third);
      originalDoc.field(fieldName, ridBagCopy, OType.LINKBAG);

      ridBag.remove(third);
      ORidBag mergedRidbag = originalDoc.field(fieldName);

      ODocumentDelta dc = doc.getDeltaFromOriginal();
      dc = dc.field("u").getValue();
      originalDoc.mergeUpdateDelta(dc);
      mergedRidbag = originalDoc.field(fieldName);
      assertEquals(ridBag, mergedRidbag);
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testRidbagsUpdateDeltaChangeWithCopy() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";

      ODocument first = new ODocument(claz);
      first = db.save(first);
      ODocument second = new ODocument(claz);
      second = db.save(second);
      ODocument third = new ODocument(claz);
      third = db.save(third);

      ORidBag ridBag = new ORidBag();
      ridBag.add(first);
      ridBag.add(second);
      ridBag.add(third);
      doc.field(fieldName, ridBag, OType.LINKBAG);
      doc = db.save(doc);

      ODocument originalDoc = new ODocument(claz);
      ORidBag ridBagCopy = new ORidBag();
      ridBagCopy.add(first);
      ridBagCopy.add(second);
      ridBagCopy.add(third);
      originalDoc.field(fieldName, ridBagCopy, OType.LINKBAG);

      ridBag = new ORidBag();
      ridBag.add(first);
      ridBag.add(third);
      doc.field(fieldName, ridBag, OType.LINKBAG);

      ODocumentDelta dc = doc.getDeltaFromOriginal();
      dc = dc.field("u").getValue();
      originalDoc.mergeUpdateDelta(dc);
      ORidBag mergedRidbag = originalDoc.field(fieldName);
      assertEquals(ridBag, mergedRidbag);
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testRidbagsUpdateDeltaAddInEmbeddedDocument() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass claz = db.createClassIfNotExist("TestClass");

      String fieldName = "testField";

      ODocument first = new ODocument(claz);
      first = db.save(first);
      ODocument second = new ODocument(claz);
      second = db.save(second);

      ORidBag ridBag = new ORidBag();
      ridBag.add(first);
      ridBag.add(second);
      ODocument doc = new ODocument(claz);
      doc.field(fieldName, ridBag, OType.LINKBAG);
      doc = db.save(doc);
      ODocument container = new ODocument(claz);
      container.field(fieldName, doc, OType.EMBEDDED);
      container = db.save(container);

      ODocument originalDoc = new ODocument(claz);
      ORidBag ridBagCopy = new ORidBag();
      ridBagCopy.add(first);
      ridBagCopy.add(second);
      originalDoc.field(fieldName, ridBagCopy, OType.LINKBAG);
      ODocument containerCopy = new ODocument(claz);
      containerCopy.field(fieldName, originalDoc, OType.EMBEDDED);

      ODocument third = new ODocument(claz);
      third = db.save(third);
      ridBag.add(third);

      ODocumentDelta dc = container.getDeltaFromOriginal();
      dc = dc.field("u").getValue();
      containerCopy.mergeUpdateDelta(dc);
      ODocument tmp = containerCopy.field(fieldName);
      ORidBag mergedRidbag = tmp.field(fieldName);
      assertEquals(ridBag, mergedRidbag);
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

}
