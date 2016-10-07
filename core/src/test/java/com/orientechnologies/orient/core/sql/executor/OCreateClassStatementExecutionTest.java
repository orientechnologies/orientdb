package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OCreateClassStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OCreateClassStatementExecutionTest");
    db.create();
  }

  @AfterClass public static void afterClass() {
    db.close();
  }

  @Test public void testPlain() {
    String className = "testPlain";
    OTodoResultSet result = db.command("create class " + className);
    OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.getClass(className);
    Assert.assertNotNull(clazz);
    Assert.assertFalse(clazz.isAbstract());
    result.close();
  }

  @Test public void testAbstract() {
    String className = "testAbstract";
    OTodoResultSet result = db.command("create class " + className + " abstract ");
    OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.getClass(className);
    Assert.assertNotNull(clazz);
    Assert.assertTrue(clazz.isAbstract());
    result.close();
  }

  @Test public void testCluster() {
    String className = "testCluster";
    OTodoResultSet result = db.command("create class " + className + " cluster 1235, 1236, 1255");
    OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.getClass(className);
    Assert.assertNotNull(clazz);
    Assert.assertFalse(clazz.isAbstract());
    Assert.assertEquals(3, clazz.getClusterIds().length);
    result.close();
  }

  @Test public void testClusters() {
    String className = "testClusters";
    OTodoResultSet result = db.command("create class " + className + " clusters 32");
    OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.getClass(className);
    Assert.assertNotNull(clazz);
    Assert.assertFalse(clazz.isAbstract());
    Assert.assertEquals(32, clazz.getClusterIds().length);
    result.close();
  }

  private void printExecutionPlan(String query, OTodoResultSet result) {
    if (query != null) {
      System.out.println(query);
    }
    result.getExecutionPlan().ifPresent(x -> System.out.println(x.prettyPrint(0, 3)));
    System.out.println();
  }

}
