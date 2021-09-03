package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OCreateClassStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OCreateClassStatementExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  @Test
  public void testPlain() {
    String className = "testPlain";
    OResultSet result = db.command("create class " + className);
    OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.getClass(className);
    Assert.assertNotNull(clazz);
    Assert.assertFalse(clazz.isAbstract());
    result.close();
  }

  @Test
  public void testAbstract() {
    String className = "testAbstract";
    OResultSet result = db.command("create class " + className + " abstract ");
    OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.getClass(className);
    Assert.assertNotNull(clazz);
    Assert.assertTrue(clazz.isAbstract());
    result.close();
  }

  @Test
  public void testCluster() {
    String className = "testCluster";
    OResultSet result = db.command("create class " + className + " cluster 1235, 1236, 1255");
    OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.getClass(className);
    Assert.assertNotNull(clazz);
    Assert.assertFalse(clazz.isAbstract());
    Assert.assertEquals(3, clazz.getClusterIds().length);
    result.close();
  }

  @Test
  public void testClusters() {
    String className = "testClusters";
    OResultSet result = db.command("create class " + className + " clusters 32");
    OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.getClass(className);
    Assert.assertNotNull(clazz);
    Assert.assertFalse(clazz.isAbstract());
    Assert.assertEquals(32, clazz.getClusterIds().length);
    result.close();
  }

  @Test
  public void testIfNotExists() {
    String className = "testIfNotExists";
    OResultSet result = db.command("create class " + className + " if not exists");
    OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.getClass(className);
    Assert.assertNotNull(clazz);
    result.close();

    result = db.command("create class " + className + " if not exists");
    clazz = schema.getClass(className);
    Assert.assertNotNull(clazz);
    result.close();
  }
}
