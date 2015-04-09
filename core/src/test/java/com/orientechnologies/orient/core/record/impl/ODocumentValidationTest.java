package com.orientechnologies.orient.core.record.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;

public class ODocumentValidationTest {

  @Test
  public void testRequiredValidation() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + ODocumentValidationTest.class.getSimpleName());
    db.create();
    try {
      ODocument doc = new ODocument();
      OIdentifiable id = db.save(doc).getIdentity();
      OClass clazz = db.getMetadata().getSchema().createClass("Validation");
      clazz.createProperty("int", OType.INTEGER).setMandatory(true);
      clazz.createProperty("long", OType.LONG).setMandatory(true);
      clazz.createProperty("float", OType.FLOAT).setMandatory(true);
      clazz.createProperty("boolean", OType.BOOLEAN).setMandatory(true);
      clazz.createProperty("binary", OType.BINARY).setMandatory(true);
      clazz.createProperty("byte", OType.BYTE).setMandatory(true);
      clazz.createProperty("date", OType.DATE).setMandatory(true);
      clazz.createProperty("datetime", OType.DATETIME).setMandatory(true);
      clazz.createProperty("decimal", OType.DECIMAL).setMandatory(true);
      clazz.createProperty("double", OType.DOUBLE).setMandatory(true);
      clazz.createProperty("short", OType.SHORT).setMandatory(true);
      clazz.createProperty("string", OType.STRING).setMandatory(true);
      clazz.createProperty("link", OType.LINK).setMandatory(true);
      clazz.createProperty("embedded", OType.EMBEDDED).setMandatory(true);

      clazz.createProperty("embeddedList", OType.EMBEDDEDLIST).setMandatory(true);
      clazz.createProperty("embeddedSet", OType.EMBEDDEDSET).setMandatory(true);
      clazz.createProperty("embeddedMap", OType.EMBEDDEDMAP).setMandatory(true);

      clazz.createProperty("linkList", OType.LINKLIST).setMandatory(true);
      clazz.createProperty("linkSet", OType.LINKSET).setMandatory(true);
      clazz.createProperty("linkMap", OType.LINKMAP).setMandatory(true);

      ODocument d = new ODocument(clazz);
      d.field("int", 10);
      d.field("long", 10);
      d.field("float", 10);
      d.field("boolean", 10);
      d.field("binary", new byte[] {});
      d.field("byte", 10);
      d.field("date", new Date());
      d.field("datetime", new Date());
      d.field("decimal", 10);
      d.field("double", 10);
      d.field("short", 10);
      d.field("string", "yeah");
      d.field("link", id);
      d.field("embedded", new ODocument().field("test", "test"));
      d.field("embeddedList", new ArrayList<String>());
      d.field("embeddedSet", new HashSet<String>());
      d.field("embeddedMap", new HashMap<String, String>());
      d.field("linkList", new ArrayList<ORecordId>());
      d.field("linkSet", new HashSet<ORecordId>());
      d.field("linkMap", new HashMap<String, ORecordId>());
      d.validate();

      checkRequireField(d, "int");
      checkRequireField(d, "long");
      checkRequireField(d, "float");
      checkRequireField(d, "boolean");
      checkRequireField(d, "binary");
      checkRequireField(d, "byte");
      checkRequireField(d, "date");
      checkRequireField(d, "datetime");
      checkRequireField(d, "decimal");
      checkRequireField(d, "double");
      checkRequireField(d, "short");
      checkRequireField(d, "string");
      checkRequireField(d, "link");
      checkRequireField(d, "embedded");
      checkRequireField(d, "embeddedList");
      checkRequireField(d, "embeddedSet");
      checkRequireField(d, "embeddedMap");
      checkRequireField(d, "linkList");
      checkRequireField(d, "linkSet");
      checkRequireField(d, "linkMap");

    } finally {
      db.drop();
    }

  }

  private void checkRequireField(ODocument toCheck, String fieldName) {
    try {
      ODocument newD = toCheck.copy();
      newD.removeField(fieldName);
      newD.validate();
      Assert.fail();
    } catch (OValidationException v) {
    }
  }

  @Test
  public void testMaxValidation() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + ODocumentValidationTest.class.getSimpleName());
    db.create();
    try {
      ODocument doc = new ODocument();
      OIdentifiable id = db.save(doc).getIdentity();
      OClass clazz = db.getMetadata().getSchema().createClass("Validation");
      clazz.createProperty("int", OType.INTEGER).setMax("11");
      clazz.createProperty("long", OType.LONG).setMax("11");
      clazz.createProperty("float", OType.FLOAT).setMax("11");
      // clazz.createProperty("boolean", OType.BOOLEAN).setMax("11");
      clazz.createProperty("binary", OType.BINARY).setMax("11");
      clazz.createProperty("byte", OType.BYTE).setMax("11");
      // clazz.createProperty("date", OType.DATE).setMandatory(true);
      // clazz.createProperty("datetime", OType.DATETIME).setMandatory(true);
      clazz.createProperty("decimal", OType.DECIMAL).setMax("11");
      clazz.createProperty("double", OType.DOUBLE).setMax("11");
      clazz.createProperty("short", OType.SHORT).setMax("11");
      clazz.createProperty("string", OType.STRING).setMax("11");
      // clazz.createProperty("link", OType.LINK).setMandatory(true);
      // clazz.createProperty("embedded", OType.EMBEDDED).setMandatory(true);

      // clazz.createProperty("embeddedList", OType.EMBEDDEDLIST).setMandatory(true);
      // clazz.createProperty("embeddedSet", OType.EMBEDDEDSET).setMandatory(true);
      // clazz.createProperty("embeddedMap", OType.EMBEDDEDMAP).setMandatory(true);

      // clazz.createProperty("linkList", OType.LINKLIST).setMandatory(true);
      // clazz.createProperty("linkSet", OType.LINKSET).setMandatory(true);
      // clazz.createProperty("linkMap", OType.LINKMAP).setMandatory(true);

      ODocument d = new ODocument(clazz);
      d.field("int", 10);
      d.field("long", 10);
      d.field("float", 10);
      // d.field("boolean", 10);
      d.field("binary", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
      d.field("byte", 10);
      // d.field("date", new Date());
      // d.field("datetime", new Date());
      d.field("decimal", 10);
      d.field("double", 10);
      d.field("short", 10);
      d.field("string", "yeah");
      d.field("link", id);
      // d.field("embedded", new ODocument().field("test", "test"));
      // d.field("embeddedList", new ArrayList<String>());
      // d.field("embeddedSet", new HashSet<String>());
      // d.field("embeddedMap", new HashMap<String, String>());
      // d.field("linkList", new ArrayList<ORecordId>());
      // d.field("linkSet", new HashSet<ORecordId>());
      // d.field("linkMap", new HashMap<String, ORecordId>());
      d.validate();

      checkField(d, "int", 20);
      checkField(d, "long", 20);
      checkField(d, "float", 20);
      // checkMaxField(d, "boolean");
      checkField(d, "binary", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 });
      // checkMaxField(d, "byte", 20);
      // checkMaxField(d, "date");
      // checkMaxField(d, "datetime");
      // checkMaxField(d, "decimal", 20);
      checkField(d, "double", 20);
      // checkMaxField(d, "short", 20);
      checkField(d, "string", "0123456789101112");
      // checkMaxField(d, "link");
      // checkMaxField(d, "embedded");
      // checkMaxField(d, "embeddedList");
      // checkMaxField(d, "embeddedSet");
      // checkMaxField(d, "embeddedMap");
      // checkMaxField(d, "linkList");
      // checkMaxField(d, "linkSet");
      // checkMaxField(d, "linkMap");

    } finally {
      db.drop();
    }

  }

  @Test
  public void testMinValidation() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + ODocumentValidationTest.class.getSimpleName());
    db.create();
    try {
      ODocument doc = new ODocument();
      OIdentifiable id = db.save(doc).getIdentity();
      OClass clazz = db.getMetadata().getSchema().createClass("Validation");
      clazz.createProperty("int", OType.INTEGER).setMin("11");
      clazz.createProperty("long", OType.LONG).setMin("11");
      clazz.createProperty("float", OType.FLOAT).setMin("11");
      // clazz.createProperty("boolean", OType.BOOLEAN).setMax("11");
      clazz.createProperty("binary", OType.BINARY).setMin("11");
      clazz.createProperty("byte", OType.BYTE).setMin("11");
      // clazz.createProperty("date", OType.DATE).setMandatory(true);
      // clazz.createProperty("datetime", OType.DATETIME).setMandatory(true);
      clazz.createProperty("decimal", OType.DECIMAL).setMin("11");
      clazz.createProperty("double", OType.DOUBLE).setMin("11");
      clazz.createProperty("short", OType.SHORT).setMin("11");
      clazz.createProperty("string", OType.STRING).setMin("11");
      // clazz.createProperty("link", OType.LINK).setMandatory(true);
      // clazz.createProperty("embedded", OType.EMBEDDED).setMandatory(true);

      // clazz.createProperty("embeddedList", OType.EMBEDDEDLIST).setMandatory(true);
      // clazz.createProperty("embeddedSet", OType.EMBEDDEDSET).setMandatory(true);
      // clazz.createProperty("embeddedMap", OType.EMBEDDEDMAP).setMandatory(true);

      // clazz.createProperty("linkList", OType.LINKLIST).setMandatory(true);
      // clazz.createProperty("linkSet", OType.LINKSET).setMandatory(true);
      // clazz.createProperty("linkMap", OType.LINKMAP).setMandatory(true);

      ODocument d = new ODocument(clazz);
      d.field("int", 12);
      d.field("long", 12);
      d.field("float", 12);
      // d.field("boolean", 10);
      d.field("binary", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 });
      d.field("byte", 12);
      // d.field("date", new Date());
      // d.field("datetime", new Date());
      d.field("decimal", 12);
      d.field("double", 12);
      d.field("short", 12);
      d.field("string", "yeahyeahyeah");
      d.field("link", id);
      // d.field("embedded", new ODocument().field("test", "test"));
      // d.field("embeddedList", new ArrayList<String>());
      // d.field("embeddedSet", new HashSet<String>());
      // d.field("embeddedMap", new HashMap<String, String>());
      // d.field("linkList", new ArrayList<ORecordId>());
      // d.field("linkSet", new HashSet<ORecordId>());
      // d.field("linkMap", new HashMap<String, ORecordId>());
      d.validate();

      checkField(d, "int", 10);
      checkField(d, "long", 10);
      checkField(d, "float", 10);
      // checkMaxField(d, "boolean");
      checkField(d, "binary", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
      // checkMaxField(d, "byte", 20);
      // checkMaxField(d, "date");
      // checkMaxField(d, "datetime");
      // checkMaxField(d, "decimal", 20);
      checkField(d, "double", 10);
      // checkMaxField(d, "short", 20);
      checkField(d, "string", "01234");
      // checkMaxField(d, "link");
      // checkMaxField(d, "embedded");
      // checkMaxField(d, "embeddedList");
      // checkMaxField(d, "embeddedSet");
      // checkMaxField(d, "embeddedMap");
      // checkMaxField(d, "linkList");
      // checkMaxField(d, "linkSet");
      // checkMaxField(d, "linkMap");

    } finally {
      db.drop();
    }

  }

  @Test
  public void testNotNullValidation() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + ODocumentValidationTest.class.getSimpleName());
    db.create();
    try {
      ODocument doc = new ODocument();
      OIdentifiable id = db.save(doc).getIdentity();
      OClass clazz = db.getMetadata().getSchema().createClass("Validation");
      clazz.createProperty("int", OType.INTEGER).setNotNull(true);
      clazz.createProperty("long", OType.LONG).setNotNull(true);
      clazz.createProperty("float", OType.FLOAT).setNotNull(true);
      clazz.createProperty("boolean", OType.BOOLEAN).setNotNull(true);
      clazz.createProperty("binary", OType.BINARY).setNotNull(true);
      clazz.createProperty("byte", OType.BYTE).setNotNull(true);
      clazz.createProperty("date", OType.DATE).setNotNull(true);
      clazz.createProperty("datetime", OType.DATETIME).setNotNull(true);
      clazz.createProperty("decimal", OType.DECIMAL).setNotNull(true);
      clazz.createProperty("double", OType.DOUBLE).setNotNull(true);
      clazz.createProperty("short", OType.SHORT).setNotNull(true);
      clazz.createProperty("string", OType.STRING).setNotNull(true);
      clazz.createProperty("link", OType.LINK).setNotNull(true);
      clazz.createProperty("embedded", OType.EMBEDDED).setNotNull(true);

      clazz.createProperty("embeddedList", OType.EMBEDDEDLIST).setNotNull(true);
      clazz.createProperty("embeddedSet", OType.EMBEDDEDSET).setNotNull(true);
      clazz.createProperty("embeddedMap", OType.EMBEDDEDMAP).setNotNull(true);

      clazz.createProperty("linkList", OType.LINKLIST).setNotNull(true);
      clazz.createProperty("linkSet", OType.LINKSET).setNotNull(true);
      clazz.createProperty("linkMap", OType.LINKMAP).setNotNull(true);

      ODocument d = new ODocument(clazz);
      d.field("int", 12);
      d.field("long", 12);
      d.field("float", 12);
      d.field("boolean", true);
      d.field("binary", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 });
      d.field("byte", 12);
      d.field("date", new Date());
      d.field("datetime", new Date());
      d.field("decimal", 12);
      d.field("double", 12);
      d.field("short", 12);
      d.field("string", "yeah");
      d.field("link", id);
      d.field("embedded", new ODocument().field("test", "test"));
      d.field("embeddedList", new ArrayList<String>());
      d.field("embeddedSet", new HashSet<String>());
      d.field("embeddedMap", new HashMap<String, String>());
      d.field("linkList", new ArrayList<ORecordId>());
      d.field("linkSet", new HashSet<ORecordId>());
      d.field("linkMap", new HashMap<String, ORecordId>());
      d.validate();

      checkField(d, "int", null);
      checkField(d, "long", null);
      checkField(d, "float", null);
      checkField(d, "boolean", null);
      checkField(d, "binary", null);
      checkField(d, "byte", null);
      checkField(d, "date", null);
      checkField(d, "datetime", null);
      checkField(d, "decimal", null);
      checkField(d, "double", null);
      checkField(d, "short", null);
      checkField(d, "string", null);
      checkField(d, "link", null);
      checkField(d, "embedded", null);
      checkField(d, "embeddedList", null);
      checkField(d, "embeddedSet", null);
      checkField(d, "embeddedMap", null);
      checkField(d, "linkList", null);
      checkField(d, "linkSet", null);
      checkField(d, "linkMap", null);

    } finally {
      db.drop();
    }

  }

  @Test
  public void testRegExpValidation() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + ODocumentValidationTest.class.getSimpleName());
    db.create();
    try {
      OClass clazz = db.getMetadata().getSchema().createClass("Validation");
      clazz.createProperty("string", OType.STRING).setRegexp("[^Z]*");

      ODocument d = new ODocument(clazz);
      d.field("string", "yeah");
      d.validate();

      checkField(d, "string", "yaZah");

    } finally {
      db.drop();
    }

  }

  @Test
  public void testLinkedTypeValidation() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + ODocumentValidationTest.class.getSimpleName());
    db.create();
    try {
      OClass clazz = db.getMetadata().getSchema().createClass("Validation");
      clazz.createProperty("embeddedList", OType.EMBEDDEDLIST).setLinkedType(OType.INTEGER);
      clazz.createProperty("embeddedSet", OType.EMBEDDEDSET).setLinkedType(OType.INTEGER);
      clazz.createProperty("embeddedMap", OType.EMBEDDEDMAP).setLinkedType(OType.INTEGER);

      ODocument d = new ODocument(clazz);
      List<Integer> list = Arrays.asList(1, 2);
      d.field("embeddedList", list);
      Set<Integer> set = new HashSet<Integer>(list);
      d.field("embeddedSet", set);

      Map<String, Integer> map = new HashMap<String, Integer>();
      map.put("a", 1);
      map.put("b", 2);
      d.field("embeddedMap", map);

      d.validate();

      checkField(d, "embeddedList", Arrays.asList("a", "b"));
      checkField(d, "embeddedSet", new HashSet<String>(Arrays.asList("a", "b")));
      Map<String, String> map1 = new HashMap<String, String>();
      map1.put("a", "a1");
      map1.put("b", "a2");
      checkField(d, "embeddedMap", map1);

    } finally {
      db.drop();
    }
  }

  @Test
  public void testLinkedClassValidation() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + ODocumentValidationTest.class.getSimpleName());
    db.create();
    try {
      OClass clazz = db.getMetadata().getSchema().createClass("Validation");
      OClass clazz1 = db.getMetadata().getSchema().createClass("Validation1");
      clazz.createProperty("linkList", OType.LINKLIST).setLinkedClass(clazz1);
      clazz.createProperty("linkSet", OType.LINKSET).setLinkedClass(clazz1);
      clazz.createProperty("linkMap", OType.LINKMAP).setLinkedClass(clazz1);
      ODocument d = new ODocument(clazz);
      List<ODocument> list = Arrays.asList(new ODocument(clazz1));
      d.field("linkList", list);
      Set<ODocument> set = new HashSet<ODocument>(list);
      d.field("linkSet", set);

      Map<String, ODocument> map = new HashMap<String, ODocument>();
      map.put("a", new ODocument(clazz1));
      d.field("linkMap", map);

      d.validate();

      checkField(d, "linkList", Arrays.asList("a", "b"));
      checkField(d, "linkSet", new HashSet<String>(Arrays.asList("a", "b")));
      Map<String, String> map1 = new HashMap<String, String>();
      map1.put("a", "a1");
      map1.put("b", "a2");
      checkField(d, "linkMap", map1);

      checkField(d, "linkList", Arrays.asList(new ODocument(clazz)));
      checkField(d, "linkSet", new HashSet<ODocument>(Arrays.asList(new ODocument(clazz))));
      Map<String, ODocument> map2 = new HashMap<String, ODocument>();
      map2.put("a", new ODocument(clazz));
      checkField(d, "linkMap", map2);

    } finally {
      db.drop();
    }
  }

  private void checkField(ODocument toCheck, String field, Object newValue) {
    try {
      ODocument newD = toCheck.copy();
      newD.field(field, newValue);
      newD.validate();
      Assert.fail();
    } catch (OValidationException v) {
    }
  }

}
