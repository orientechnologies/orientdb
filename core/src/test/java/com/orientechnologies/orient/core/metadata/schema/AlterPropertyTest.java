package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.junit.Assert.assertNotNull;
import static org.testng.AssertJUnit.*;

public class AlterPropertyTest {

  private ODatabaseDocument db;

  @BeforeMethod
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + AlterPropertyTest.class.getSimpleName());
    db.create();
  }

  @AfterMethod
  public void after() {
    db.drop();
  }

  @Test
  public void testPropertyRenaming() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("TestPropertyRenaming");
    OProperty property = classA.createProperty("propertyOld", OType.STRING);
    assertEquals(property, classA.getProperty("propertyOld"));
    assertNull(classA.getProperty("propertyNew"));
    property.setName("propertyNew");
    assertNull(classA.getProperty("propertyOld"));
    assertEquals(property, classA.getProperty("propertyNew"));
  }

  @Test
  public void testPropertyRenamingReload() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("TestPropertyRenaming");
    OProperty property = classA.createProperty("propertyOld", OType.STRING);
    assertEquals(property, classA.getProperty("propertyOld"));
    assertNull(classA.getProperty("propertyNew"));
    property.setName("propertyNew");
    schema.reload();
    classA = schema.getClass("TestPropertyRenaming");
    assertNull(classA.getProperty("propertyOld"));
    assertEquals(property, classA.getProperty("propertyNew"));
  }

  @Test
  public void testLinkedMapPropertyLinkedType() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("TestMapProperty");
    try {
      classA.createProperty("propertyMap", OType.LINKMAP, OType.STRING);
      fail("create linkmap property should not allow linked type");
    } catch (OSchemaException e) {

    }

    OProperty prop = classA.getProperty("propertyMap");
    assertNull(prop);
  }

  @Test
  public void testLinkedMapPropertyLinkedClass() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("TestMapProperty");
    OClass classLinked = schema.createClass("LinkedClass");
    try {
      classA.createProperty("propertyString", OType.STRING, classLinked);
      fail("create linkmap property should not allow linked type");
    } catch (OSchemaException e) {

    }

    OProperty prop = classA.getProperty("propertyString");
    assertNull(prop);
  }

  @Test
  public void testRemoveLinkedClass() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("TestRemoveLinkedClass");
    OClass classLinked = schema.createClass("LinkedClass");
    OProperty prop = classA.createProperty("propertyLink", OType.LINK, classLinked);
    assertNotNull(prop.getLinkedClass());
    prop.setLinkedClass(null);
    assertNull(prop.getLinkedClass());
  }

  @Test
  public void testRemoveLinkedClassSQL() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("TestRemoveLinkedClass");
    OClass classLinked = schema.createClass("LinkedClass");
    OProperty prop = classA.createProperty("propertyLink", OType.LINK, classLinked);
    assertNotNull(prop.getLinkedClass());
    db.command(new OCommandSQL("alter property TestRemoveLinkedClass.propertyLink linkedclass null")).execute();
    assertNull(prop.getLinkedClass());
  }

  @Test
  public void testMax() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("TestWrongMax");
    OProperty prop = classA.createProperty("dates", OType.EMBEDDEDLIST, OType.DATE);

    db.command(new OCommandSQL("alter property TestWrongMax.dates max 2016-05-25")).execute();

    try {
      db.command(new OCommandSQL("alter property TestWrongMax.dates max '2016-05-25'")).execute();
      Assert.fail();
    } catch (Exception e) {
    }

  }

  @Test
  public void testAlterPropertyWithDot() {

    OSchema schema = db.getMetadata().getSchema();
    db.command(new OCommandSQL("create class testAlterPropertyWithDot")).execute();
    db.command(new OCommandSQL("create property testAlterPropertyWithDot.`a.b` STRING")).execute();
    schema.reload();
    Assert.assertNotNull(schema.getClass("testAlterPropertyWithDot").getProperty("a.b"));
    db.command(new OCommandSQL("alter property testAlterPropertyWithDot.`a.b` name c")).execute();
    schema.reload();
    Assert.assertNull(schema.getClass("testAlterPropertyWithDot").getProperty("a.b"));
    Assert.assertNotNull(schema.getClass("testAlterPropertyWithDot").getProperty("c"));
  }

}
