package com.orientechnologies.orient.core.metadata.schema;

import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import com.orientechnologies.orient.core.exception.OSchemaException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

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


}
