package com.orientechnologies.orient.core.record.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializerDelta;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class ODocumentSerializerDeltaTest {
  private static final String dbName = ODocumentTest.class.getSimpleName();
  private static final String defaultDbAdminCredentials = "admin";

  @Test
  public void testGetFromOriginalSimpleDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

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
      // test serialization/deserialization
      ODocumentSerializerDelta delta = ODocumentSerializerDelta.instance();
      byte[] bytes = delta.serializeDelta(doc);
      delta.deserializeDelta(bytes, originalDoc);
      assertEquals(testValue, originalDoc.field(fieldName));
      assertNull(originalDoc.field(removeField));
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      ODocument nestedDoc = new ODocumentEmbedded(claz.getName());
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

      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      // test serialization/deserialization
      serializerDelta.deserializeDelta(bytes, originalDoc);
      nestedDoc = originalDoc.field(nestedDocField);
      assertEquals(nestedDoc.field(fieldName), testValue);
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);

      String fieldName = "testField";
      List<String> originalValue = new ArrayList<>();
      originalValue.add("one");
      originalValue.add("two");
      originalValue.add("toRemove");

      doc.field(fieldName, originalValue);

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      List<String> newArray = doc.field(fieldName);
      newArray.set(1, "three");
      newArray.remove("toRemove");

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      List checkList = originalDoc.field(fieldName);
      assertEquals("three", checkList.get(1));
      assertFalse(checkList.contains("toRemove"));
    } finally {
      if (db != null) db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testSetDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);

      String fieldName = "testField";
      Set<String> originalValue = new HashSet<>();
      originalValue.add("one");
      originalValue.add("toRemove");

      doc.field(fieldName, originalValue);

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      Set<String> newArray = doc.field(fieldName);
      newArray.add("three");
      newArray.remove("toRemove");

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      Set<String> checkSet = originalDoc.field(fieldName);
      assertTrue(checkSet.contains("three"));
      assertFalse(checkSet.contains("toRemove"));
    } finally {
      if (db != null) db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testSetOfSetsDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      Set<Set<String>> originalValue = new HashSet<>();
      for (int i = 0; i < 2; i++) {
        Set<String> containedList = new HashSet<>();
        containedList.add("one");
        containedList.add("two");
        originalValue.add(containedList);
      }

      doc.field(fieldName, originalValue);

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      Set<String> newArray = (Set<String>) ((Set) doc.field(fieldName)).iterator().next();
      newArray.add("three");

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      Set<List> checkSet = originalDoc.field(fieldName);
      assertFalse(checkSet.contains("three"));
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      List<List<String>> originalValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        List<String> containedList = new ArrayList<>();
        containedList.add("one");
        containedList.add("two");

        originalValue.add(containedList);
      }

      doc.field(fieldName, originalValue);

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      List<String> newArray = (List<String>) ((List) doc.field(fieldName)).get(0);
      newArray.set(1, "three");

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      List<List> checkList = originalDoc.field(fieldName);
      assertEquals("three", checkList.get(0).get(1));
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      String fieldName = "testField";

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);

      String constantField = "constField";
      String constValue = "ConstValue";
      String variableField = "varField";
      List<ODocument> originalValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        ODocument containedDoc = new ODocumentEmbedded();
        containedDoc.field(constantField, constValue);
        containedDoc.field(variableField, "one" + i);
        originalValue.add(containedDoc);
      }

      doc.field(fieldName, originalValue);

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      ODocument testDoc = (ODocument) ((List) doc.getProperty(fieldName)).get(1);
      testDoc.field(variableField, "two");

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      List<ODocument> checkList = originalDoc.field(fieldName);
      ODocument checkDoc = checkList.get(1);
      assertEquals(checkDoc.field(constantField), constValue);
      assertEquals(checkDoc.field(variableField), "two");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      OClass claz = db.createClassIfNotExist("TestClass");

      String constantField = "constField";
      String constValue = "ConstValue";
      String variableField = "varField";

      ODocument doc = new ODocument(claz);
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

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      ODocument d1 = originalValue.get(0).get(0);
      d1.field(variableField, "two");

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      List<List<ODocument>> checkList = originalDoc.field(fieldName);
      assertEquals("two", checkList.get(0).get(0).field(variableField));
    } catch (Exception e) {
      e.printStackTrace();
      assertNotNull(null);
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      List<List<List<String>>> originalValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        List<List<String>> containedList = new ArrayList<>();
        for (int j = 0; j < 2; j++) {
          List<String> innerList = new ArrayList<>();
          innerList.add("el1" + j + i);
          innerList.add("el2" + j + i);
          containedList.add(innerList);
        }
        originalValue.add(containedList);
      }

      doc.field(fieldName, originalValue);

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      List<String> innerList =
          (List<String>) ((List) ((List) ((List) doc.field(fieldName)).get(0)).get(0));
      innerList.set(0, "changed");

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      List<List<List<String>>> checkList = originalDoc.field(fieldName);
      assertEquals("changed", checkList.get(0).get(0).get(0));
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      String fieldName = "testField";

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);

      String constantField = "constField";
      String constValue = "ConstValue";
      String variableField = "varField";
      List<ODocument> originalValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        ODocument containedDoc = new ODocumentEmbedded();
        containedDoc.field(constantField, constValue);
        List<String> listField = new ArrayList<>();
        for (int j = 0; j < 2; j++) {
          listField.add("Some" + j);
        }
        containedDoc.field(variableField, listField);
        originalValue.add(containedDoc);
      }

      doc.field(fieldName, originalValue);

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      ODocument testDoc = (ODocument) ((List) doc.field(fieldName)).get(1);
      List<String> currentList = testDoc.field(variableField);
      currentList.set(0, "changed");
      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      List<ODocument> checkList = originalDoc.field(fieldName);
      ODocument checkDoc = checkList.get(1);
      List<String> checkInnerList = checkDoc.field(variableField);
      assertEquals("changed", checkInnerList.get(0));
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);

      String fieldName = "testField";
      List<String> originalValue = new ArrayList<>();
      originalValue.add("one");
      originalValue.add("two");
      doc.field(fieldName, originalValue);

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      List<String> newArray = doc.field(fieldName);
      newArray.add("three");

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      List checkList = originalDoc.field(fieldName);
      assertEquals(3, checkList.size());
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);

      String fieldName = "testField";
      List<List<String>> originalList = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        List<String> nestedList = new ArrayList<>();
        nestedList.add("one");
        nestedList.add("two");
        originalList.add(nestedList);
      }

      doc.field(fieldName, originalList);

      doc = db.save(doc);
      // Deep Copy is not working in this case, use toStream/fromStream as workaround.
      // ODocument originalDoc = doc.copy();
      ODocument originalDoc = new ODocument();
      originalDoc.fromStream(doc.toStream());

      List<String> newArray = (List<String>) ((List) doc.field(fieldName)).get(0);
      newArray.add("three");

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      List<List<String>> rootList = originalDoc.field(fieldName);
      List<String> checkList = rootList.get(0);
      assertEquals(3, checkList.size());
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);

      String fieldName = "testField";
      List<String> originalValue = new ArrayList<>();
      originalValue.add("one");
      originalValue.add("two");
      originalValue.add("three");

      doc.field(fieldName, originalValue);

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      List<String> newArray = (List<String>) doc.field(fieldName);
      newArray.remove(0);
      newArray.remove(0);

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      List checkList = originalDoc.field(fieldName);
      assertEquals("three", checkList.get(0));
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      String constantFieldName = "constantField";
      String testValue = "testValue";

      doc.field(constantFieldName + "1", "someValue1");
      doc.field(constantFieldName, "someValue");

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      doc.field(fieldName, testValue);

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);
      assertEquals(testValue, originalDoc.field(fieldName));
    } finally {
      if (db != null) db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testRemoveCreateDocFieldDelta() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      String constantFieldName = "constantField";
      String testValue = "testValue";

      doc.field(fieldName, testValue);
      doc.field(constantFieldName, "someValue");

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      doc.removeField(fieldName);
      doc.setProperty("other", "new");

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      assertFalse(originalDoc.containsField(fieldName));
      assertEquals(originalDoc.getProperty("other"), "new");
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

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

      rootDoc = db.save(rootDoc);
      ODocument originalDoc = rootDoc.copy();

      doc.removeField(fieldName);

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(rootDoc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      ODocument nested = originalDoc.field(nestedFieldName);
      assertFalse(nested.containsField(fieldName));
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      String fieldName = "testField";

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);

      String constantField = "constField";
      String constValue = "ConstValue";
      String variableField = "varField";
      List<ODocument> originalValue = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        ODocument containedDoc = new ODocument();
        containedDoc.field(constantField, constValue);
        containedDoc.field(variableField, "one" + i);
        originalValue.add(containedDoc);
      }

      doc.field(fieldName, originalValue);

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      ODocument testDoc = (ODocument) ((List) doc.field(fieldName)).get(1);
      testDoc.removeField(variableField);
      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      List<ODocument> checkList = originalDoc.field(fieldName);
      ODocument checkDoc = checkList.get(1);
      assertEquals(checkDoc.field(constantField), constValue);
      assertFalse(checkDoc.containsField(variableField));
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      String fieldName = "testField";
      Map<String, String> mapValue = new HashMap<>();
      mapValue.put("first", "one");
      mapValue.put("second", "two");

      doc.field(fieldName, mapValue, OType.EMBEDDEDMAP);

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      Map<String, String> containedMap = doc.field(fieldName);
      containedMap.put("first", "changed");

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      containedMap = originalDoc.field(fieldName);
      assertEquals("changed", containedMap.get("first"));
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

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

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      Map<String, String> containedMap = (Map<String, String>) ((List) doc.field(fieldName)).get(0);
      containedMap.put("first", "changed");
      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      containedMap = (Map<String, String>) ((List) originalDoc.field(fieldName)).get(0);
      assertEquals("changed", containedMap.get("first"));
      containedMap = (Map<String, String>) ((List) originalDoc.field(fieldName)).get(1);
      assertEquals("one", containedMap.get("first"));
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

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

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      Map<String, ODocument> containedMap = doc.field(fieldName);
      ODocument changeDoc = containedMap.get("first");
      changeDoc.field("f1", "changed");

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);
      containedMap = originalDoc.field(fieldName);
      ODocument containedDoc = containedMap.get("first");
      assertEquals("changed", containedDoc.field("f1"));
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

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

      doc = db.save(doc);
      ODocument originalDoc = doc.copy();

      Map<String, String> containedMap = (Map<String, String>) ((List) doc.field(fieldName)).get(0);
      containedMap.put("first", "changed");
      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      containedMap = (Map<String, String>) ((List) originalDoc.field(fieldName)).get(0);
      assertEquals("changed", containedMap.get("first"));
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

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

      ODocument originalDoc = doc.save();

      ODocument third = new ODocument(claz);
      third = db.save(third);
      ridBag = new ORidBag();
      ridBag.add(first);
      ridBag.add(second);
      ridBag.add(third);
      doc.field(fieldName, ridBag, OType.LINKBAG);
      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      ORidBag mergedRidbag = originalDoc.field(fieldName);
      assertEquals(ridBag, mergedRidbag);
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

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
      ODocument originalDoc = doc.copy();

      ridBag = new ORidBag();
      ridBag.add(first);
      ridBag.add(second);
      doc.field(fieldName, ridBag, OType.LINKBAG);

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      ORidBag mergedRidbag = originalDoc.field(fieldName);
      assertEquals(ridBag, mergedRidbag);
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

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

      ODocument originalDoc = doc.copy();

      ODocument third = new ODocument(claz);
      third = db.save(third);
      ridBag.add(third);

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      ORidBag mergedRidbag = originalDoc.field(fieldName);
      assertEquals(ridBag, mergedRidbag);
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

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

      ODocument originalDoc = doc.copy();
      ridBag.remove(third);

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      ORidBag mergedRidbag = originalDoc.field(fieldName);
      assertEquals(ridBag, mergedRidbag);
    } finally {
      if (db != null) db.close();
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
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

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

      ODocument originalDoc = doc.copy();

      ridBag = new ORidBag();
      ridBag.add(first);
      ridBag.add(third);
      doc.field(fieldName, ridBag, OType.LINKBAG);

      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);

      ORidBag mergedRidbag = originalDoc.field(fieldName);
      assertEquals(ridBag, mergedRidbag);
    } finally {
      if (db != null) db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testDeltaNullValues() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      doc.setProperty("one", "value");
      doc.setProperty("list", Arrays.asList("test"));
      doc.setProperty("set", new HashSet<>(Arrays.asList("test")));
      Map<String, String> map = new HashMap<>();
      map.put("two", "value");
      doc.setProperty("map", map);
      OIdentifiable link = db.save(new ODocument("testClass"));
      doc.setProperty("linkList", Arrays.asList(link));
      doc.setProperty("linkSet", new HashSet<>(Arrays.asList(link)));
      Map<String, OIdentifiable> linkMap = new HashMap<>();
      linkMap.put("two", link);
      doc.setProperty("linkMap", linkMap);
      doc = db.save(doc);

      ODocument originalDoc = doc.copy();
      doc.setProperty("one", null);
      ((List<String>) doc.getProperty("list")).add(null);
      ((Set<String>) doc.getProperty("set")).add(null);
      ((Map<String, String>) doc.getProperty("map")).put("nullValue", null);
      ((List<OIdentifiable>) doc.getProperty("linkList")).add(null);
      ((Set<OIdentifiable>) doc.getProperty("linkSet")).add(null);
      ((Map<String, OIdentifiable>) doc.getProperty("linkMap")).put("nullValue", null);
      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);
      assertTrue(((List) originalDoc.getProperty("list")).contains(null));
      assertTrue(((Set) originalDoc.getProperty("set")).contains(null));
      assertTrue(((Map) originalDoc.getProperty("map")).containsKey("nullValue"));
      assertTrue(((List) originalDoc.getProperty("linkList")).contains(null));
      assertTrue(((Set) originalDoc.getProperty("linkSet")).contains(null));
      assertTrue(((Map) originalDoc.getProperty("linkMap")).containsKey("nullValue"));
    } finally {
      if (db != null) db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testDeltaLinkAllCases() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      OIdentifiable link = db.save(new ODocument("testClass"));
      OIdentifiable link1 = db.save(new ODocument("testClass"));
      doc.setProperty("linkList", Arrays.asList(link, link1, link1));
      doc.setProperty("linkSet", new HashSet<>(Arrays.asList(link, link1)));
      Map<String, OIdentifiable> linkMap = new HashMap<>();
      linkMap.put("one", link);
      linkMap.put("two", link1);
      linkMap.put("three", link1);
      doc.setProperty("linkMap", linkMap);
      doc = db.save(doc);

      OIdentifiable link2 = db.save(new ODocument("testClass"));

      ODocument originalDoc = doc.copy();
      ((List<OIdentifiable>) doc.getProperty("linkList")).set(1, link2);
      ((List<OIdentifiable>) doc.getProperty("linkList")).remove(link1);
      ((List<OIdentifiable>) doc.getProperty("linkList")).add(link2);
      ((Set<OIdentifiable>) doc.getProperty("linkSet")).add(link2);
      ((Set<OIdentifiable>) doc.getProperty("linkSet")).remove(link1);
      ((Map<String, OIdentifiable>) doc.getProperty("linkMap")).put("new", link2);
      ((Map<String, OIdentifiable>) doc.getProperty("linkMap")).put("three", link2);
      ((Map<String, OIdentifiable>) doc.getProperty("linkMap")).remove("two");
      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);
      assertFalse(((List) originalDoc.getProperty("linkList")).contains(link1));
      assertTrue(((List) originalDoc.getProperty("linkList")).contains(link2));
      assertEquals(((List) originalDoc.getProperty("linkList")).get(1), link2);
      assertTrue(((Set) originalDoc.getProperty("linkSet")).contains(link2));
      assertFalse(((Set) originalDoc.getProperty("linkSet")).contains(link1));
      assertEquals(((Map) originalDoc.getProperty("linkMap")).get("new"), link2);
      assertEquals(((Map) originalDoc.getProperty("linkMap")).get("three"), link2);
      assertTrue(((Map) originalDoc.getProperty("linkMap")).containsKey("one"));
      assertFalse(((Map) originalDoc.getProperty("linkMap")).containsKey("two"));
    } finally {
      if (db != null) db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testDeltaAllCasesMap() {
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = OCreateDatabaseUtil.createDatabase(dbName, "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      OClass claz = db.createClassIfNotExist("TestClass");

      ODocument doc = new ODocument(claz);
      Map<String, String> map = new HashMap<>();
      map.put("two", "value");
      doc.setProperty("map", map);
      Map<String, String> map1 = new HashMap<>();
      map1.put("two", "value");
      map1.put("one", "other");
      Map<String, Map<String, String>> mapNested = new HashMap<>();
      Map<String, String> nested = new HashMap<>();
      nested.put("one", "value");
      mapNested.put("nest", nested);
      doc.setProperty("mapNested", mapNested);
      doc.setProperty("map1", map1);
      Map<String, OElement> mapEmbedded = new HashMap<>();
      OElement embedded = db.newEmbeddedElement();
      embedded.setProperty("other", 1);
      mapEmbedded.put("first", embedded);
      doc.setProperty("mapEmbedded", mapEmbedded);
      doc = db.save(doc);

      ODocument originalDoc = doc.copy();
      OElement embedded1 = db.newEmbeddedElement();
      embedded1.setProperty("other", 1);
      ((Map<String, OElement>) doc.getProperty("mapEmbedded")).put("newDoc", embedded1);
      ((Map<String, String>) doc.getProperty("map")).put("value", "other");
      ((Map<String, String>) doc.getProperty("map")).put("two", "something");
      ((Map<String, String>) doc.getProperty("map1")).remove("one");
      ((Map<String, String>) doc.getProperty("map1")).put("two", "something");
      ((Map<String, Map<String, String>>) doc.getProperty("mapNested"))
          .get("nest")
          .put("other", "value");
      // test serialization/deserialization
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();

      byte[] bytes = serializerDelta.serializeDelta(doc);
      serializerDelta.deserializeDelta(bytes, originalDoc);
      assertNotNull(((Map) originalDoc.getProperty("mapEmbedded")).get("newDoc"));
      assertEquals(
          ((Map<String, OElement>) originalDoc.getProperty("mapEmbedded"))
              .get("newDoc")
              .getProperty("other"),
          Integer.valueOf(1));
      assertEquals(((Map) originalDoc.getProperty("map")).get("value"), "other");
      assertEquals(((Map) originalDoc.getProperty("map")).get("two"), "something");
      assertEquals(((Map) originalDoc.getProperty("map1")).get("two"), "something");
      assertNull(((Map) originalDoc.getProperty("map1")).get("one"));
      assertEquals(
          ((Map<String, Map<String, String>>) originalDoc.getProperty("mapNested"))
              .get("nest")
              .get("other"),
          "value");
    } finally {
      if (db != null) db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
  }

  @Test
  public void testSimpleSerialization() {
    ODatabaseRecordThreadLocal.instance().remove();
    ODocument document = new ODocument();

    document.field("name", "name");
    document.field("age", 20);
    document.field("youngAge", (short) 20);
    document.field("oldAge", (long) 20);
    document.field("heigth", 12.5f);
    document.field("bitHeigth", 12.5d);
    document.field("class", (byte) 'C');
    document.field("nullField", (Object) null);
    document.field("character", 'C');
    document.field("alive", true);
    document.field("dateTime", new Date());
    document.field(
        "bigNumber", new BigDecimal("43989872423376487952454365232141525434.32146432321442534"));
    ORidBag bag = new ORidBag();
    bag.add(new ORecordId(1, 1));
    bag.add(new ORecordId(2, 2));
    // document.field("ridBag", bag);
    Calendar c = Calendar.getInstance();
    document.field("date", c.getTime(), OType.DATE);
    Calendar c1 = Calendar.getInstance();
    c1.set(Calendar.MILLISECOND, 0);
    c1.set(Calendar.SECOND, 0);
    c1.set(Calendar.MINUTE, 0);
    c1.set(Calendar.HOUR_OF_DAY, 0);
    document.field("date1", c1.getTime(), OType.DATE);

    byte[] byteValue = new byte[10];
    Arrays.fill(byteValue, (byte) 10);
    document.field("bytes", byteValue);

    document.field("utf8String", new String("A" + "\u00ea" + "\u00f1" + "\u00fc" + "C"));
    document.field("recordId", new ORecordId(10, 10));

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
    ODocument extr = new ODocument();
    serializerDelta.deserialize(res, extr);

    c.set(Calendar.MILLISECOND, 0);
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.HOUR_OF_DAY, 0);

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("name"), document.field("name"));
    assertEquals(extr.<Object>field("age"), document.field("age"));
    assertEquals(extr.<Object>field("youngAge"), document.field("youngAge"));
    assertEquals(extr.<Object>field("oldAge"), document.field("oldAge"));
    assertEquals(extr.<Object>field("heigth"), document.field("heigth"));
    assertEquals(extr.<Object>field("bitHeigth"), document.field("bitHeigth"));
    assertEquals(extr.<Object>field("class"), document.field("class"));
    // TODO fix char management issue:#2427
    // assertEquals(document.field("character"), extr.field("character"));
    assertEquals(extr.<Object>field("alive"), document.field("alive"));
    assertEquals(extr.<Object>field("dateTime"), document.field("dateTime"));
    assertEquals(extr.field("date"), c.getTime());
    assertEquals(extr.field("date1"), c1.getTime());
    //    assertEquals(extr.<String>field("bytes"), document.field("bytes"));
    Assertions.assertThat(extr.<Object>field("bytes")).isEqualTo(document.field("bytes"));
    assertEquals(extr.<String>field("utf8String"), document.field("utf8String"));
    assertEquals(extr.<Object>field("recordId"), document.field("recordId"));
    assertEquals(extr.<Object>field("bigNumber"), document.field("bigNumber"));
    assertNull(extr.field("nullField"));
    // assertEquals(extr.field("ridBag"), document.field("ridBag"));

  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testSimpleLiteralArray() {
    ODatabaseRecordThreadLocal.instance().remove();
    ODocument document = new ODocument();
    String[] strings = new String[3];
    strings[0] = "a";
    strings[1] = "b";
    strings[2] = "c";
    document.field("listStrings", strings);

    Short[] shorts = new Short[3];
    shorts[0] = (short) 1;
    shorts[1] = (short) 2;
    shorts[2] = (short) 3;
    document.field("shorts", shorts);

    Long[] longs = new Long[3];
    longs[0] = (long) 1;
    longs[1] = (long) 2;
    longs[2] = (long) 3;
    document.field("longs", longs);

    Integer[] ints = new Integer[3];
    ints[0] = 1;
    ints[1] = 2;
    ints[2] = 3;
    document.field("integers", ints);

    Float[] floats = new Float[3];
    floats[0] = 1.1f;
    floats[1] = 2.2f;
    floats[2] = 3.3f;
    document.field("floats", floats);

    Double[] doubles = new Double[3];
    doubles[0] = 1.1d;
    doubles[1] = 2.2d;
    doubles[2] = 3.3d;
    document.field("doubles", doubles);

    Date[] dates = new Date[3];
    dates[0] = new Date();
    dates[1] = new Date();
    dates[2] = new Date();
    document.field("dates", dates);

    Byte[] bytes = new Byte[3];
    bytes[0] = (byte) 0;
    bytes[1] = (byte) 1;
    bytes[2] = (byte) 3;
    document.field("bytes", bytes);

    // TODO: char not currently supported in orient.
    Character[] chars = new Character[3];
    chars[0] = 'A';
    chars[1] = 'B';
    chars[2] = 'C';
    // document.field("chars", chars);

    Boolean[] booleans = new Boolean[3];
    booleans[0] = true;
    booleans[1] = false;
    booleans[2] = false;
    document.field("booleans", booleans);

    Object[] arrayNulls = new Object[3];
    // document.field("arrayNulls", arrayNulls);

    // Object[] listMixed = new ArrayList[9];
    // listMixed[0] = new Boolean(true);
    // listMixed[1] = 1;
    // listMixed[2] = (long) 5;
    // listMixed[3] = (short) 2;
    // listMixed[4] = 4.0f;
    // listMixed[5] = 7.0D;
    // listMixed[6] = "hello";
    // listMixed[7] = new Date();
    // listMixed[8] = (byte) 10;
    // document.field("listMixed", listMixed);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
    ODocument extr = new ODocument();
    serializerDelta.deserialize(res, extr);

    assertEquals(extr.fields(), document.fields());
    assertEquals(((List) extr.field("listStrings")).toArray(), document.field("listStrings"));
    assertEquals(((List) extr.field("integers")).toArray(), document.field("integers"));
    assertEquals(((List) extr.field("doubles")).toArray(), document.field("doubles"));
    assertEquals(((List) extr.field("dates")).toArray(), document.field("dates"));
    assertEquals(((List) extr.field("bytes")).toArray(), document.field("bytes"));
    assertEquals(((List) extr.field("booleans")).toArray(), document.field("booleans"));
    // assertEquals(((List) extr.field("arrayNulls")).toArray(), document.field("arrayNulls"));
    // assertEquals(extr.field("listMixed"), document.field("listMixed"));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testSimpleLiteralList() {
    ODatabaseRecordThreadLocal.instance().remove();
    ODocument document = new ODocument();
    List<String> strings = new ArrayList<String>();
    strings.add("a");
    strings.add("b");
    strings.add("c");
    document.field("listStrings", strings);

    List<Short> shorts = new ArrayList<Short>();
    shorts.add((short) 1);
    shorts.add((short) 2);
    shorts.add((short) 3);
    document.field("shorts", shorts);

    List<Long> longs = new ArrayList<Long>();
    longs.add((long) 1);
    longs.add((long) 2);
    longs.add((long) 3);
    document.field("longs", longs);

    List<Integer> ints = new ArrayList<Integer>();
    ints.add(1);
    ints.add(2);
    ints.add(3);
    document.field("integers", ints);

    List<Float> floats = new ArrayList<Float>();
    floats.add(1.1f);
    floats.add(2.2f);
    floats.add(3.3f);
    document.field("floats", floats);

    List<Double> doubles = new ArrayList<Double>();
    doubles.add(1.1);
    doubles.add(2.2);
    doubles.add(3.3);
    document.field("doubles", doubles);

    List<Date> dates = new ArrayList<Date>();
    dates.add(new Date());
    dates.add(new Date());
    dates.add(new Date());
    document.field("dates", dates);

    List<Byte> bytes = new ArrayList<Byte>();
    bytes.add((byte) 0);
    bytes.add((byte) 1);
    bytes.add((byte) 3);
    document.field("bytes", bytes);

    // TODO: char not currently supported in orient.
    List<Character> chars = new ArrayList<Character>();
    chars.add('A');
    chars.add('B');
    chars.add('C');
    // document.field("chars", chars);

    List<Boolean> booleans = new ArrayList<Boolean>();
    booleans.add(true);
    booleans.add(false);
    booleans.add(false);
    document.field("booleans", booleans);

    List listMixed = new ArrayList();
    listMixed.add(true);
    listMixed.add(1);
    listMixed.add((long) 5);
    listMixed.add((short) 2);
    listMixed.add(4.0f);
    listMixed.add(7.0D);
    listMixed.add("hello");
    listMixed.add(new Date());
    listMixed.add((byte) 10);
    document.field("listMixed", listMixed);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
    ODocument extr = new ODocument();
    serializerDelta.deserialize(res, extr);

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("listStrings"), document.field("listStrings"));
    assertEquals(extr.<Object>field("integers"), document.field("integers"));
    assertEquals(extr.<Object>field("doubles"), document.field("doubles"));
    assertEquals(extr.<Object>field("dates"), document.field("dates"));
    assertEquals(extr.<Object>field("bytes"), document.field("bytes"));
    assertEquals(extr.<Object>field("booleans"), document.field("booleans"));
    assertEquals(extr.<Object>field("listMixed"), document.field("listMixed"));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testSimpleLiteralSet() throws InterruptedException {
    ODatabaseRecordThreadLocal.instance().remove();
    ODocument document = new ODocument();
    Set<String> strings = new HashSet<String>();
    strings.add("a");
    strings.add("b");
    strings.add("c");
    document.field("listStrings", strings);

    Set<Short> shorts = new HashSet<Short>();
    shorts.add((short) 1);
    shorts.add((short) 2);
    shorts.add((short) 3);
    document.field("shorts", shorts);

    Set<Long> longs = new HashSet<Long>();
    longs.add((long) 1);
    longs.add((long) 2);
    longs.add((long) 3);
    document.field("longs", longs);

    Set<Integer> ints = new HashSet<Integer>();
    ints.add(1);
    ints.add(2);
    ints.add(3);
    document.field("integers", ints);

    Set<Float> floats = new HashSet<Float>();
    floats.add(1.1f);
    floats.add(2.2f);
    floats.add(3.3f);
    document.field("floats", floats);

    Set<Double> doubles = new HashSet<Double>();
    doubles.add(1.1);
    doubles.add(2.2);
    doubles.add(3.3);
    document.field("doubles", doubles);

    Set<Date> dates = new HashSet<Date>();
    dates.add(new Date());
    Thread.sleep(1);
    dates.add(new Date());
    Thread.sleep(1);
    dates.add(new Date());
    document.field("dates", dates);

    Set<Byte> bytes = new HashSet<Byte>();
    bytes.add((byte) 0);
    bytes.add((byte) 1);
    bytes.add((byte) 3);
    document.field("bytes", bytes);

    // TODO: char not currently supported in orient.
    Set<Character> chars = new HashSet<Character>();
    chars.add('A');
    chars.add('B');
    chars.add('C');
    // document.field("chars", chars);

    Set<Boolean> booleans = new HashSet<Boolean>();
    booleans.add(true);
    booleans.add(false);
    booleans.add(false);
    document.field("booleans", booleans);

    Set listMixed = new HashSet();
    listMixed.add(true);
    listMixed.add(1);
    listMixed.add((long) 5);
    listMixed.add((short) 2);
    listMixed.add(4.0f);
    listMixed.add(7.0D);
    listMixed.add("hello");
    listMixed.add(new Date());
    listMixed.add((byte) 10);
    listMixed.add(new ORecordId(10, 20));
    document.field("listMixed", listMixed);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
    ODocument extr = new ODocument();
    serializerDelta.deserialize(res, extr);

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("listStrings"), document.field("listStrings"));
    assertEquals(extr.<Object>field("integers"), document.field("integers"));
    assertEquals(extr.<Object>field("doubles"), document.field("doubles"));
    assertEquals(extr.<Object>field("dates"), document.field("dates"));
    assertEquals(extr.<Object>field("bytes"), document.field("bytes"));
    assertEquals(extr.<Object>field("booleans"), document.field("booleans"));
    assertEquals(extr.<Object>field("listMixed"), document.field("listMixed"));
  }

  @Test
  public void testLinkCollections() {
    ODatabaseDocument db =
        new ODatabaseDocumentTx("memory:ODocumentSchemalessBinarySerializationTest").create();
    try {
      ODocument document = new ODocument();
      Set<ORecordId> linkSet = new HashSet<ORecordId>();
      linkSet.add(new ORecordId(10, 20));
      linkSet.add(new ORecordId(10, 21));
      linkSet.add(new ORecordId(10, 22));
      linkSet.add(new ORecordId(11, 22));
      document.field("linkSet", linkSet, OType.LINKSET);

      List<ORecordId> linkList = new ArrayList<ORecordId>();
      linkList.add(new ORecordId(10, 20));
      linkList.add(new ORecordId(10, 21));
      linkList.add(new ORecordId(10, 22));
      linkList.add(new ORecordId(11, 22));
      document.field("linkList", linkList, OType.LINKLIST);

      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
      byte[] res = serializerDelta.serialize(document);
      ODocument extr = new ODocument();
      serializerDelta.deserialize(res, extr);

      assertEquals(extr.fields(), document.fields());
      assertEquals(
          ((Set<?>) extr.field("linkSet")).size(), ((Set<?>) document.field("linkSet")).size());
      assertTrue(((Set<?>) extr.field("linkSet")).containsAll((Set<?>) document.field("linkSet")));
      assertEquals(extr.<Object>field("linkList"), document.field("linkList"));
    } finally {
      db.drop();
    }
  }

  @Test
  public void testSimpleEmbeddedDoc() {
    ODatabaseRecordThreadLocal.instance().remove();
    ODocument document = new ODocument();
    ODocument embedded = new ODocument();
    embedded.field("name", "test");
    embedded.field("surname", "something");
    document.field("embed", embedded, OType.EMBEDDED);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
    ODocument extr = new ODocument();
    serializerDelta.deserialize(res, extr);

    assertEquals(document.fields(), extr.fields());
    ODocument emb = extr.field("embed");
    assertNotNull(emb);
    assertEquals(emb.<Object>field("name"), embedded.field("name"));
    assertEquals(emb.<Object>field("surname"), embedded.field("surname"));
  }

  @Test
  public void testSimpleMapStringLiteral() {
    ODatabaseRecordThreadLocal.instance().remove();
    ODocument document = new ODocument();

    Map<String, String> mapString = new HashMap<String, String>();
    mapString.put("key", "value");
    mapString.put("key1", "value1");
    document.field("mapString", mapString);

    Map<String, Integer> mapInt = new HashMap<String, Integer>();
    mapInt.put("key", 2);
    mapInt.put("key1", 3);
    document.field("mapInt", mapInt);

    Map<String, Long> mapLong = new HashMap<String, Long>();
    mapLong.put("key", 2L);
    mapLong.put("key1", 3L);
    document.field("mapLong", mapLong);

    Map<String, Short> shortMap = new HashMap<String, Short>();
    shortMap.put("key", (short) 2);
    shortMap.put("key1", (short) 3);
    document.field("shortMap", shortMap);

    Map<String, Date> dateMap = new HashMap<String, Date>();
    dateMap.put("key", new Date());
    dateMap.put("key1", new Date());
    document.field("dateMap", dateMap);

    Map<String, Float> floatMap = new HashMap<String, Float>();
    floatMap.put("key", 10f);
    floatMap.put("key1", 11f);
    document.field("floatMap", floatMap);

    Map<String, Double> doubleMap = new HashMap<String, Double>();
    doubleMap.put("key", 10d);
    doubleMap.put("key1", 11d);
    document.field("doubleMap", doubleMap);

    Map<String, Byte> bytesMap = new HashMap<String, Byte>();
    bytesMap.put("key", (byte) 10);
    bytesMap.put("key1", (byte) 11);
    document.field("bytesMap", bytesMap);

    Map<String, String> mapWithNulls = new HashMap<String, String>();
    mapWithNulls.put("key", "dddd");
    mapWithNulls.put("key1", null);
    document.field("bytesMap", mapWithNulls);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
    ODocument extr = new ODocument();
    serializerDelta.deserialize(res, extr);

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("mapString"), document.field("mapString"));
    assertEquals(extr.<Object>field("mapLong"), document.field("mapLong"));
    assertEquals(extr.<Object>field("shortMap"), document.field("shortMap"));
    assertEquals(extr.<Object>field("dateMap"), document.field("dateMap"));
    assertEquals(extr.<Object>field("doubleMap"), document.field("doubleMap"));
    assertEquals(extr.<Object>field("bytesMap"), document.field("bytesMap"));
  }

  @Test
  public void testlistOfList() {
    ODatabaseRecordThreadLocal.instance().remove();
    ODocument document = new ODocument();
    List<List<String>> list = new ArrayList<List<String>>();
    List<String> ls = new ArrayList<String>();
    ls.add("test1");
    ls.add("test2");
    list.add(ls);
    document.field("complexList", list);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
    ODocument extr = new ODocument();
    serializerDelta.deserialize(res, extr);

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("complexList"), document.field("complexList"));
  }

  @Test
  public void testArrayOfArray() {
    ODatabaseRecordThreadLocal.instance().remove();
    ODocument document = new ODocument();
    String[][] array = new String[1][];
    String[] ls = new String[2];
    ls[0] = "test1";
    ls[1] = "test2";
    array[0] = ls;
    document.field("complexArray", array);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
    ODocument extr = new ODocument();
    serializerDelta.deserialize(res, extr);

    assertEquals(extr.fields(), document.fields());
    List<List<String>> savedValue = extr.field("complexArray");
    assertEquals(savedValue.size(), array.length);
    assertEquals(savedValue.get(0).size(), array[0].length);
    assertEquals(savedValue.get(0).get(0), array[0][0]);
    assertEquals(savedValue.get(0).get(1), array[0][1]);
  }

  @Test
  public void testEmbeddedListOfEmbeddedMap() {
    ODatabaseRecordThreadLocal.instance().remove();

    ODocument document = new ODocument();
    List<Map<String, String>> coll = new ArrayList<Map<String, String>>();
    Map<String, String> map = new HashMap<String, String>();
    map.put("first", "something");
    map.put("second", "somethingElse");
    Map<String, String> map2 = new HashMap<String, String>();
    map2.put("first", "something");
    map2.put("second", "somethingElse");
    coll.add(map);
    coll.add(map2);
    document.field("list", coll);
    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
    ODocument extr = new ODocument();
    serializerDelta.deserialize(res, extr);
    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("list"), document.field("list"));
  }

  @Test
  public void testMapOfEmbeddedDocument() {
    ODatabaseRecordThreadLocal.instance().remove();

    ODocument document = new ODocument();

    ODocument embeddedInMap = new ODocument();
    embeddedInMap.field("name", "test");
    embeddedInMap.field("surname", "something");
    Map<String, ODocument> map = new HashMap<String, ODocument>();
    map.put("embedded", embeddedInMap);
    document.field("map", map, OType.EMBEDDEDMAP);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
    ODocument extr = new ODocument();
    serializerDelta.deserialize(res, extr);

    Map<String, ODocument> mapS = extr.field("map");
    assertEquals(1, mapS.size());
    ODocument emb = mapS.get("embedded");
    assertNotNull(emb);
    assertEquals(emb.<Object>field("name"), embeddedInMap.field("name"));
    assertEquals(emb.<Object>field("surname"), embeddedInMap.field("surname"));
  }

  @Test
  public void testMapOfLink() {
    // needs a database because of the lazy loading
    ODatabaseDocument db =
        new ODatabaseDocumentTx("memory:ODocumentSchemalessBinarySerializationTest").create();
    try {
      ODocument document = new ODocument();

      Map<String, OIdentifiable> map = new HashMap<String, OIdentifiable>();
      map.put("link", new ORecordId(0, 0));
      document.field("map", map, OType.LINKMAP);

      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
      byte[] res = serializerDelta.serialize(document);
      ODocument extr = new ODocument();
      serializerDelta.deserialize(res, extr);

      assertEquals(extr.fields(), document.fields());
      assertEquals(extr.<Object>field("map"), document.field("map"));
    } finally {
      db.drop();
    }
  }

  @Test
  public void testDocumentSimple() {
    ODatabaseDocument db =
        new ODatabaseDocumentTx("memory:ODocumentSchemalessBinarySerializationTest").create();
    try {
      ODocument document = new ODocument("TestClass");
      document.field("test", "test");
      ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
      byte[] res = serializerDelta.serialize(document);
      ODocument extr = new ODocument();
      serializerDelta.deserialize(res, extr);

      //      assertEquals(extr.getClassName(), document.getClassName());
      assertEquals(extr.fields(), document.fields());
      assertEquals(extr.<Object>field("test"), document.field("test"));
    } finally {
      db.drop();
    }
  }

  @Test
  public void testDocumentWithCostum() {
    boolean old = OGlobalConfiguration.DB_CUSTOM_SUPPORT.getValueAsBoolean();
    OGlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(true);
    ODatabaseRecordThreadLocal.instance().remove();
    ODocument document = new ODocument();
    document.field("test", "test");
    document.field("custom", new ODocumentSchemalessBinarySerializationTest.Custom());

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
    ODocument extr = new ODocument();
    serializerDelta.deserialize(res, extr);

    assertEquals(extr.getClassName(), document.getClassName());
    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("test"), document.field("test"));
    assertEquals(extr.<Object>field("custom"), document.field("custom"));
    OGlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(old);
  }

  @Test
  public void testDocumentWithCostumDocument() {
    ODatabaseRecordThreadLocal.instance().remove();
    ODocument document = new ODocument();
    document.field("test", "test");
    document.field("custom", new ODocumentSchemalessBinarySerializationTest.CustomDocument());

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
    ODocument extr = new ODocument();
    serializerDelta.deserialize(res, extr);

    assertEquals(extr.getClassName(), document.getClassName());
    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("test"), document.field("test"));
    assertEquals(extr.<Object>field("custom"), document.field("custom"));
  }

  @Test(expected = OSerializationException.class)
  public void testSetOfWrongData() {
    ODatabaseRecordThreadLocal.instance().remove();

    ODocument document = new ODocument();

    Set<Object> embeddedSet = new HashSet<Object>();
    embeddedSet.add(new WrongData());
    document.field("embeddedSet", embeddedSet, OType.EMBEDDEDSET);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
  }

  @Test(expected = OSerializationException.class)
  public void testListOfWrongData() {
    ODatabaseRecordThreadLocal.instance().remove();

    ODocument document = new ODocument();

    List<Object> embeddedList = new ArrayList<Object>();
    embeddedList.add(new WrongData());
    document.field("embeddedList", embeddedList, OType.EMBEDDEDLIST);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
  }

  @Test(expected = OSerializationException.class)
  public void testMapOfWrongData() {
    ODatabaseRecordThreadLocal.instance().remove();

    ODocument document = new ODocument();

    Map<String, Object> embeddedMap = new HashMap<String, Object>();
    embeddedMap.put("name", new WrongData());
    document.field("embeddedMap", embeddedMap, OType.EMBEDDEDMAP);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
  }

  @Test(expected = ClassCastException.class)
  public void testLinkSetOfWrongData() {
    ODatabaseRecordThreadLocal.instance().remove();

    ODocument document = new ODocument();

    Set<Object> linkSet = new HashSet<Object>();
    linkSet.add(new WrongData());
    document.field("linkSet", linkSet, OType.LINKSET);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
  }

  @Test(expected = ClassCastException.class)
  public void testLinkListOfWrongData() {
    ODatabaseRecordThreadLocal.instance().remove();

    ODocument document = new ODocument();

    List<Object> linkList = new ArrayList<Object>();
    linkList.add(new WrongData());
    document.field("linkList", linkList, OType.LINKLIST);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
  }

  @Test(expected = ClassCastException.class)
  public void testLinkMapOfWrongData() {
    ODatabaseRecordThreadLocal.instance().remove();

    ODocument document = new ODocument();

    Map<String, Object> linkMap = new HashMap<String, Object>();
    linkMap.put("name", new WrongData());
    document.field("linkMap", linkMap, OType.LINKMAP);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
  }

  @Test(expected = OSerializationException.class)
  public void testFieldWrongData() {
    ODatabaseRecordThreadLocal.instance().remove();

    ODocument document = new ODocument();

    document.field("wrongData", new WrongData());

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
  }

  @Test
  public void testCollectionOfEmbeddedDocument() {

    ODatabaseRecordThreadLocal.instance().remove();

    ODocument document = new ODocument();

    ODocument embeddedInList = new ODocument();
    embeddedInList.field("name", "test");
    embeddedInList.field("surname", "something");

    ODocument embeddedInList2 = new ODocument();
    embeddedInList2.field("name", "test1");
    embeddedInList2.field("surname", "something2");

    List<ODocument> embeddedList = new ArrayList<ODocument>();
    embeddedList.add(embeddedInList);
    embeddedList.add(embeddedInList2);
    embeddedList.add(null);
    embeddedList.add(new ODocument());
    document.field("embeddedList", embeddedList, OType.EMBEDDEDLIST);

    ODocument embeddedInSet = new ODocument();
    embeddedInSet.field("name", "test2");
    embeddedInSet.field("surname", "something3");

    ODocument embeddedInSet2 = new ODocument();
    embeddedInSet2.field("name", "test5");
    embeddedInSet2.field("surname", "something6");

    Set<ODocument> embeddedSet = new HashSet<ODocument>();
    embeddedSet.add(embeddedInSet);
    embeddedSet.add(embeddedInSet2);
    embeddedSet.add(new ODocument());
    document.field("embeddedSet", embeddedSet, OType.EMBEDDEDSET);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
    ODocument extr = new ODocument();
    serializerDelta.deserialize(res, extr);

    List<ODocument> ser = extr.field("embeddedList");
    assertEquals(ser.size(), 4);
    assertNotNull(ser.get(0));
    assertNotNull(ser.get(1));
    assertNull(ser.get(2));
    assertNotNull(ser.get(3));
    ODocument inList = ser.get(0);
    assertNotNull(inList);
    assertEquals(inList.<Object>field("name"), embeddedInList.field("name"));
    assertEquals(inList.<Object>field("surname"), embeddedInList.field("surname"));

    Set<ODocument> setEmb = extr.field("embeddedSet");
    assertEquals(setEmb.size(), 3);
    boolean ok = false;
    for (ODocument inSet : setEmb) {
      assertNotNull(inSet);
      if (embeddedInSet.field("name").equals(inSet.field("name"))
          && embeddedInSet.field("surname").equals(inSet.field("surname"))) ok = true;
    }
    assertTrue("not found record in the set after serilize", ok);
  }

  @Test
  public void testSerializableValue() {
    boolean old = OGlobalConfiguration.DB_CUSTOM_SUPPORT.getValueAsBoolean();
    OGlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(true);

    ODocument document = new ODocument();
    SimpleSerializableClass ser = new SimpleSerializableClass();
    ser.name = "testName";
    document.field("seri", ser);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
    ODocument extr = new ODocument();
    serializerDelta.deserialize(res, extr);

    assertNotNull(extr.field("seri"));
    assertEquals(extr.fieldType("seri"), OType.CUSTOM);
    SimpleSerializableClass newser = extr.field("seri");
    assertEquals(newser.name, ser.name);
    OGlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(old);
  }

  @Test
  public void testFieldNames() {
    ODocument document = new ODocument();
    document.fields("a", 1, "b", 2, "c", 3);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
    ODocument extr = new ODocument();
    serializerDelta.deserialize(res, extr);

    final String[] fields = extr.fieldNames();

    assertNotNull(fields);
    assertEquals(fields.length, 3);
    assertEquals(fields[0], "a");
    assertEquals(fields[1], "b");
    assertEquals(fields[2], "c");
  }

  @Test
  public void testWithRemove() {
    ODocument document = new ODocument();
    document.field("name", "name");
    document.field("age", 20);
    document.field("youngAge", (short) 20);
    document.field("oldAge", (long) 20);
    document.removeField("oldAge");

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
    ODocument extr = new ODocument();
    serializerDelta.deserialize(res, extr);

    assertEquals(document.field("name"), extr.<Object>field("name"));
    assertEquals(document.<Object>field("age"), extr.field("age"));
    assertEquals(document.<Object>field("youngAge"), extr.field("youngAge"));
    assertNull(extr.field("oldAge"));
  }

  @Test
  public void testListOfMapsWithNull() {
    ODatabaseRecordThreadLocal.instance().remove();
    ODocument document = new ODocument();

    List lista = new ArrayList<>();
    Map mappa = new LinkedHashMap<>();
    mappa.put("prop1", "val1");
    mappa.put("prop2", null);
    lista.add(mappa);

    mappa = new HashMap();
    mappa.put("prop", "val");
    lista.add(mappa);
    document.setProperty("list", lista);

    ODocumentSerializerDelta serializerDelta = ODocumentSerializerDelta.instance();
    byte[] res = serializerDelta.serialize(document);
    ODocument extr = new ODocument();
    serializerDelta.deserialize(res, extr);

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("list"), document.field("list"));
  }

  public static class Custom implements OSerializableStream {
    byte[] bytes = new byte[10];

    @Override
    public OSerializableStream fromStream(byte[] iStream) throws OSerializationException {
      bytes = iStream;
      return this;
    }

    @Override
    public byte[] toStream() throws OSerializationException {
      for (int i = 0; i < bytes.length; i++) {
        bytes[i] = (byte) i;
      }
      return bytes;
    }

    @Override
    public boolean equals(Object obj) {
      return obj != null
          && obj instanceof ODocumentSchemalessBinarySerializationTest.Custom
          && Arrays.equals(bytes, ((ODocumentSchemalessBinarySerializationTest.Custom) obj).bytes);
    }
  }

  public static class CustomDocument implements ODocumentSerializable {
    private ODocument document;

    @Override
    public void fromDocument(ODocument document) {
      this.document = document;
    }

    @Override
    public ODocument toDocument() {
      document = new ODocument();
      document.field("test", "some strange content");
      return document;
    }

    @Override
    public boolean equals(Object obj) {
      return obj != null
          && document.field("test").equals(((CustomDocument) obj).document.field("test"));
    }
  }

  private class WrongData {}
}
