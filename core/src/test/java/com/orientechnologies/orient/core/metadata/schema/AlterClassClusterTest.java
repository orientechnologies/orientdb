package com.orientechnologies.orient.core.metadata.schema;

import static org.testng.AssertJUnit.assertEquals;

import com.orientechnologies.orient.core.exception.OSchemaException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

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
    OClass clazz = db.getMetadata().getSchema().createClass("Test", 1, null);
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


}
