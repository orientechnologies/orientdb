package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OCreateClassStatementExecutionTest extends BaseMemoryDatabase {

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
