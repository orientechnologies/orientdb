package com.orientechnologies.orient.core.metadata.schema;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSchemaException;

public class OSchemaSharedGlobalPropertyTest {

  private ODatabaseDocumentTx db;

  @BeforeMethod
  public void setUp() {
    db = new ODatabaseDocumentTx("memory:" + OSchemaSharedGlobalPropertyTest.class.getSimpleName());
    if (db.exists()) {
      db.open("admin", "admin");
    } else
      db.create();
  }

  @AfterMethod
  public void after() {
    db.close();
  }

  @AfterClass
  public void afterClass() {
    db.open("admin", "admin");
    db.drop();
  }

  @Test
  public void testGlobalPropertyCreate() {

    OSchema schema = db.getMetadata().getSchema();

    schema.createGlobalProperty("testaasd", OType.SHORT, 100);
    OGlobalProperty prop = schema.getGlobalPropertyById(100);
    assertEquals(prop.getName(), "testaasd");
    assertEquals(prop.getId(), (Integer) 100);
    assertEquals(prop.getType(), OType.SHORT);

  }

  @Test
  public void testGlobalPropertyCreateDoubleSame() {

    OSchema schema = db.getMetadata().getSchema();

    schema.createGlobalProperty("test", OType.SHORT, 200);
    schema.createGlobalProperty("test", OType.SHORT, 200);

  }

  @Test(expectedExceptions = OSchemaException.class)
  public void testGlobalPropertyCreateDouble() {

    OSchema schema = db.getMetadata().getSchema();

    schema.createGlobalProperty("test", OType.SHORT, 201);
    schema.createGlobalProperty("test1", OType.SHORT, 201);

  }
  
}
