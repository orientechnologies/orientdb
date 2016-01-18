package com.orientechnologies.orient.core.metadata.schema;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.AssertJUnit.assertEquals;

import com.orientechnologies.orient.core.exception.OSchemaException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

import java.util.Arrays;

public class AlterClassClusterTest {

  private ODatabaseDocument db;

  @BeforeMethod
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + AlterClassClusterTest.class.getSimpleName());
    db.create();
  }

  @AfterMethod
  public void after() {
    db.drop();
  }

  @Test
  public void testRemoveClusterDefaultCluster() {
    OClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.addCluster("TestOneMore");

    clazz.removeClusterId(db.getClusterIdByName("Test"));
    db.getMetadata().getSchema().reload();
    clazz = db.getMetadata().getSchema().getClass("Test");
    assertEquals(clazz.getDefaultClusterId(), db.getClusterIdByName("TestOneMore"));

    clazz.removeClusterId(db.getClusterIdByName("TestOneMore"));
    assertEquals(clazz.getDefaultClusterId(), -1);
  }


  @Test(expectedExceptions = OSchemaException.class)
  public void testAddClusterToAbstracClass() {
    OClass clazz = db.getMetadata().getSchema().createAbstractClass("Test");
    clazz.addCluster("TestOneMore");

  }

  @Test(expectedExceptions = OSchemaException.class)
  public void testAddClusterIdToAbstracClass() {
    OClass clazz = db.getMetadata().getSchema().createAbstractClass("Test");
    int id = db.addCluster("TestOneMore");
    clazz.addClusterId(id);
  }


  @Test
  public void testPolimorficClusterAbstractClass() {
    OClass clazz = db.getMetadata().getSchema().createAbstractClass("Test");
    int id = db.addCluster("TestOneMore");
    ((OClassImpl) clazz).addClusterIdInternal(id);

    db.getMetadata().getSchema().reload();
    clazz = db.getMetadata().getSchema().getClass("Test");

    for(int cur :clazz.getPolymorphicClusterIds()){
      assertNotEquals(cur,-1,"cluster id -1 not allowed in polimorfic clusters");
    }

  }


}

