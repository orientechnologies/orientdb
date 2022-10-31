package com.orientechnologies.orient.core.metadata.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.exception.OSchemaException;
import org.junit.Assert;
import org.junit.Test;

public class AlterPropertyTest extends BaseMemoryDatabase {

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
    db.command("alter property TestRemoveLinkedClass.propertyLink linkedclass null").close();
    assertNull(prop.getLinkedClass());
  }

  @Test
  public void testMax() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("TestWrongMax");
    OProperty prop = classA.createProperty("dates", OType.EMBEDDEDLIST, OType.DATE);

    db.command("alter property TestWrongMax.dates max 2016-05-25").close();

    try {
      db.command("alter property TestWrongMax.dates max '2016-05-25'").close();
      Assert.fail();
    } catch (Exception e) {
    }
  }

  @Test
  public void testAlterPropertyWithDot() {

    OSchema schema = db.getMetadata().getSchema();
    db.command("create class testAlterPropertyWithDot").close();
    db.command("create property testAlterPropertyWithDot.`a.b` STRING").close();
    schema.reload();
    Assert.assertNotNull(schema.getClass("testAlterPropertyWithDot").getProperty("a.b"));
    db.command("alter property testAlterPropertyWithDot.`a.b` name c").close();
    schema.reload();
    Assert.assertNull(schema.getClass("testAlterPropertyWithDot").getProperty("a.b"));
    Assert.assertNotNull(schema.getClass("testAlterPropertyWithDot").getProperty("c"));
  }

  @Test
  public void testAlterCustomAttributeInProperty() {
    OSchema schema = db.getMetadata().getSchema();
    OClass oClass = schema.createClass("TestCreateCustomAttributeClass");
    OProperty property = oClass.createProperty("property", OType.STRING);

    property.setCustom("customAttribute", "value1");
    assertEquals("value1", property.getCustom("customAttribute"));

    property.setCustom("custom.attribute", "value2");
    assertEquals("value2", property.getCustom("custom.attribute"));
  }
}
