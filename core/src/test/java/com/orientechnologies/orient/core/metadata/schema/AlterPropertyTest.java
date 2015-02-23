package com.orientechnologies.orient.core.metadata.schema;

import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertEquals;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

public class AlterPropertyTest {

  @Test
  public void testPropertyRenaming() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + AlterPropertyTest.class.getSimpleName());
    db.create();
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("TestPropertyRenaming");
    OProperty property = classA.createProperty("propertyOld", OType.STRING);
    assertEquals(property, classA.getProperty("propertyOld"));
    assertNull(classA.getProperty("propertyNew"));
    property.setName("propertyNew");
    assertNull(classA.getProperty("propertyOld"));
    assertEquals(property, classA.getProperty("propertyNew"));
    db.drop();
  }

  @Test
  public void testPropertyRenamingReload() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + AlterPropertyTest.class.getSimpleName());
    db.create();
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
    db.drop();
  }
}
