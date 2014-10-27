package com.orientechnologies.orient.core.record.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;

public class ODocumentFieldConversionTest {

  private ODatabaseDocument db;
  private OClass            clazz;

  @BeforeTest
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + this.getClass().getSimpleName());
    db.create();
    clazz = db.getMetadata().getSchema().createClass("testClass");
    clazz.createProperty("integer", OType.INTEGER);
    clazz.createProperty("string", OType.STRING);
    clazz.createProperty("boolean", OType.BOOLEAN);
    clazz.createProperty("long", OType.LONG);
    clazz.createProperty("float", OType.FLOAT);
    clazz.createProperty("double", OType.DOUBLE);
    clazz.createProperty("decimal", OType.DECIMAL);
    clazz.createProperty("date", OType.DATE);
  }

  @Test
  public void testDateToSchemaConversion() {
    ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseRecordInternal) ((ODatabaseDocumentTx) db).getUnderlying());
    Calendar calendare = Calendar.getInstance();
    calendare.set(Calendar.MILLISECOND, 0);
    Date date = calendare.getTime();

    String dateString = ((ODatabaseDocumentTx) db).getStorage().getConfiguration().getDateTimeFormatInstance().format(date);
    ODocument doc = new ODocument(clazz);
    doc.field("date", dateString);
    assertTrue(doc.field("date") instanceof Date);
    assertEquals(date, doc.field("date"));

    doc.field("date", 20304);
    assertTrue(doc.field("date") instanceof Date);
    assertEquals(20304L, ((Date) doc.field("date")).getTime());

    doc.field("date", 43432440f);
    assertTrue(doc.field("date") instanceof Date);
    assertEquals(43432440L, ((Date) doc.field("date")).getTime());

    doc.field("date", 43432444D);
    assertTrue(doc.field("date") instanceof Date);
    assertEquals(43432444L, ((Date) doc.field("date")).getTime());

    doc.field("date", 20304L);
    assertTrue(doc.field("date") instanceof Date);
    assertEquals(20304L, ((Date) doc.field("date")).getTime());

    ODatabaseRecordThreadLocal.INSTANCE.remove();
  }

  @Test
  public void testLiteralToSchemaConvertionInteger() {
    ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseRecordInternal) ((ODatabaseDocumentTx) db).getUnderlying());
    ODocument doc = new ODocument(clazz);
    doc.field("integer", 2L);
    assertTrue(doc.field("integer") instanceof Integer);
    assertEquals(2, doc.field("integer"));

    doc.field("integer", 3f);
    assertTrue(doc.field("integer") instanceof Integer);
    assertEquals(3, doc.field("integer"));

    doc.field("integer", 4d);
    assertTrue(doc.field("integer") instanceof Integer);
    assertEquals(4, doc.field("integer"));

    doc.field("integer", "5");
    assertTrue(doc.field("integer") instanceof Integer);
    assertEquals(5, doc.field("integer"));

    doc.field("integer", new BigDecimal("6"));
    assertTrue(doc.field("integer") instanceof Integer);
    assertEquals(6, doc.field("integer"));

    // doc.field("integer", true);
    // assertTrue(doc.field("integer") instanceof Integer);
    // assertEquals(1, doc.field("integer"));
    ODatabaseRecordThreadLocal.INSTANCE.remove();

  }

  @Test
  public void testLiteralToSchemaConvertionString() {
    ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseRecordInternal) ((ODatabaseDocumentTx) db).getUnderlying());
    ODocument doc = new ODocument(clazz);

    doc.field("string", 1);
    assertTrue(doc.field("string") instanceof String);
    assertEquals("1", doc.field("string"));

    doc.field("string", 2L);
    assertTrue(doc.field("string") instanceof String);
    assertEquals("2", doc.field("string"));

    doc.field("string", 3f);
    assertTrue(doc.field("string") instanceof String);
    assertEquals("3.0", doc.field("string"));

    doc.field("string", 4d);
    assertTrue(doc.field("string") instanceof String);
    assertEquals("4.0", doc.field("string"));

    doc.field("string", new BigDecimal("6"));
    assertTrue(doc.field("string") instanceof String);
    assertEquals("6", doc.field("string"));

    doc.field("string", true);
    assertTrue(doc.field("string") instanceof String);
    assertEquals("true", doc.field("string"));
    ODatabaseRecordThreadLocal.INSTANCE.remove();
  }

  @Test
  public void testLiteralToSchemaConvertionFloat() {
    ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseRecordInternal) ((ODatabaseDocumentTx) db).getUnderlying());
    ODocument doc = new ODocument(clazz);

    doc.field("float", 1);
    assertTrue(doc.field("float") instanceof Float);
    assertEquals(1f, doc.field("float"));

    doc.field("float", 2L);
    assertTrue(doc.field("float") instanceof Float);
    assertEquals(2f, doc.field("float"));

    doc.field("float", "3");
    assertTrue(doc.field("float") instanceof Float);
    assertEquals(3f, doc.field("float"));

    doc.field("float", 4d);
    assertTrue(doc.field("float") instanceof Float);
    assertEquals(4f, doc.field("float"));

    doc.field("float", new BigDecimal("6"));
    assertTrue(doc.field("float") instanceof Float);
    assertEquals(6f, doc.field("float"));

    // doc.field("float", true);
    // assertTrue(doc.field("float") instanceof Float);
    // assertEquals(1f, doc.field("float"));
    ODatabaseRecordThreadLocal.INSTANCE.remove();
  }

  @Test
  public void testLiteralToSchemaConvertionDouble() {
    ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseRecordInternal) ((ODatabaseDocumentTx) db).getUnderlying());
    ODocument doc = new ODocument(clazz);

    doc.field("double", 1);
    assertTrue(doc.field("double") instanceof Double);
    assertEquals(1d, doc.field("double"));

    doc.field("double", 2L);
    assertTrue(doc.field("double") instanceof Double);
    assertEquals(2d, doc.field("double"));

    doc.field("double", "3");
    assertTrue(doc.field("double") instanceof Double);
    assertEquals(3d, doc.field("double"));

    doc.field("double", 4f);
    assertTrue(doc.field("double") instanceof Double);
    assertEquals(4d, doc.field("double"));

    doc.field("double", new BigDecimal("6"));
    assertTrue(doc.field("double") instanceof Double);
    assertEquals(6d, doc.field("double"));

    // doc.field("double", true);
    // assertTrue(doc.field("double") instanceof Double);
    // assertEquals(1d, doc.field("double"));
    ODatabaseRecordThreadLocal.INSTANCE.remove();
  }

  @Test
  public void testLiteralToSchemaConvertionLong() {
    ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseRecordInternal) ((ODatabaseDocumentTx) db).getUnderlying());
    ODocument doc = new ODocument(clazz);

    doc.field("long", 1);
    assertTrue(doc.field("long") instanceof Long);
    assertEquals(1L, doc.field("long"));

    doc.field("long", 2f);
    assertTrue(doc.field("long") instanceof Long);
    assertEquals(2L, doc.field("long"));

    doc.field("long", "3");
    assertTrue(doc.field("long") instanceof Long);
    assertEquals(3L, doc.field("long"));

    doc.field("long", 4d);
    assertTrue(doc.field("long") instanceof Long);
    assertEquals(4L, doc.field("long"));

    doc.field("long", new BigDecimal("6"));
    assertTrue(doc.field("long") instanceof Long);
    assertEquals(6L, doc.field("long"));

    // doc.field("long", true);
    // assertTrue(doc.field("long") instanceof Long);
    // assertEquals(1, doc.field("long"));
    ODatabaseRecordThreadLocal.INSTANCE.remove();
  }

  @Test
  public void testLiteralToSchemaConvertionBoolean() {
    ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseRecordInternal) ((ODatabaseDocumentTx) db).getUnderlying());
    ODocument doc = new ODocument(clazz);

    doc.field("boolean", 0);
    assertTrue(doc.field("boolean") instanceof Boolean);
    assertEquals(false, doc.field("boolean"));

    doc.field("boolean", 1L);
    assertTrue(doc.field("boolean") instanceof Boolean);
    assertEquals(true, doc.field("boolean"));

    doc.field("boolean", 2f);
    assertTrue(doc.field("boolean") instanceof Boolean);
    assertEquals(true, doc.field("boolean"));

    doc.field("boolean", "true");
    assertTrue(doc.field("boolean") instanceof Boolean);
    assertEquals(true, doc.field("boolean"));

    doc.field("boolean", 4d);
    assertTrue(doc.field("boolean") instanceof Boolean);
    assertEquals(true, doc.field("boolean"));

    doc.field("boolean", new BigDecimal("6"));
    assertTrue(doc.field("boolean") instanceof Boolean);
    assertEquals(true, doc.field("boolean"));
    ODatabaseRecordThreadLocal.INSTANCE.remove();
  }

  @Test
  public void testLiteralToSchemaConvertionDecimal() {
    ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseRecordInternal) ((ODatabaseDocumentTx) db).getUnderlying());
    ODocument doc = new ODocument(clazz);

    doc.field("decimal", 0);
    assertTrue(doc.field("decimal") instanceof BigDecimal);
    assertEquals(BigDecimal.ZERO, doc.field("decimal"));

    doc.field("decimal", 1L);
    assertTrue(doc.field("decimal") instanceof BigDecimal);
    assertEquals(BigDecimal.ONE, doc.field("decimal"));

    doc.field("decimal", 2f);
    assertTrue(doc.field("decimal") instanceof BigDecimal);
    assertEquals(new BigDecimal("2.0"), doc.field("decimal"));

    doc.field("decimal", "3");
    assertTrue(doc.field("decimal") instanceof BigDecimal);
    assertEquals(new BigDecimal("3"), doc.field("decimal"));

    doc.field("decimal", 4d);
    assertTrue(doc.field("decimal") instanceof BigDecimal);
    assertEquals(new BigDecimal("4.0"), doc.field("decimal"));

    doc.field("boolean", new BigDecimal("6"));
    assertTrue(doc.field("boolean") instanceof Boolean);
    assertEquals(true, doc.field("boolean"));
    ODatabaseRecordThreadLocal.INSTANCE.remove();
  }

  @Test
  public void testConversionAlsoWithWrongType() {
    ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseRecordInternal) ((ODatabaseDocumentTx) db).getUnderlying());
    ODocument doc = new ODocument(clazz);

    doc.field("float", 2, OType.INTEGER);
    assertTrue(doc.field("float") instanceof Float);
    assertEquals(2f, doc.field("float"));

    doc.field("integer", 3f, OType.FLOAT);
    assertTrue(doc.field("integer") instanceof Integer);
    assertEquals(3, doc.field("integer"));

    doc.field("double", 1l, OType.LONG);
    assertTrue(doc.field("double") instanceof Double);
    assertEquals(1d, doc.field("double"));

    doc.field("long", 1d, OType.DOUBLE);
    assertTrue(doc.field("long") instanceof Long);
    assertEquals(1L, doc.field("long"));
    ODatabaseRecordThreadLocal.INSTANCE.remove();
  }

  @Test
  public void testLiteralConversionAfterSchemaSet() {
    ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseRecordInternal) ((ODatabaseDocumentTx) db).getUnderlying());
    ODocument doc = new ODocument();

    doc.field("float", 1);
    doc.field("integer", 3f);
    doc.field("double", 2L);
    doc.field("long", 2D);
    doc.field("string", 25);
    doc.field("boolean", "true");
    doc.field("decimal", -1);
    doc.field("date", 20304L);

    doc.setClass(clazz);
    assertTrue(doc.field("float") instanceof Float);
    assertEquals(1f, doc.field("float"));

    assertTrue(doc.field("integer") instanceof Integer);
    assertEquals(3, doc.field("integer"));

    assertTrue(doc.field("long") instanceof Long);
    assertEquals(2L, doc.field("long"));

    assertTrue(doc.field("string") instanceof String);
    assertEquals("25", doc.field("string"));

    assertTrue(doc.field("boolean") instanceof Boolean);
    assertEquals(true, doc.field("boolean"));

    assertTrue(doc.field("decimal") instanceof BigDecimal);
    assertEquals(new BigDecimal(-1), doc.field("decimal"));

    assertTrue(doc.field("date") instanceof Date);
    assertEquals(20304L, ((Date) doc.field("date")).getTime());

    ODatabaseRecordThreadLocal.INSTANCE.remove();
  }

  @AfterTest
  public void after() {
    db.drop();
  }

}
