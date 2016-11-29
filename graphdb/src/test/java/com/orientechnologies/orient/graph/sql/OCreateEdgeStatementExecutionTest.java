package com.orientechnologies.orient.graph.sql;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.ExecutionPlanPrintUtils;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OTodoResultSet;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OCreateEdgeStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OCreateEdgeStatementExecutionTest");
    db.create();
    OClass v = db.getMetadata().getSchema().getClass("V");
    if (v == null) {
      db.getMetadata().getSchema().createClass("V");
    }
    OClass e = db.getMetadata().getSchema().getClass("E");
    if (e == null) {
      db.getMetadata().getSchema().createClass("E");
    }
  }

  @AfterClass public static void afterClass() {
    db.drop();
  }

  @Test public void testCreateSingleEdge() {
    OSchema schema = db.getMetadata().getSchema();

    String vClass = "testCreateSingleEdgeV";
    schema.createClass(vClass, schema.getClass("V"));

    String eClass = "testCreateSingleEdgeE";
    schema.createClass(eClass, schema.getClass("E"));

    OVertex v1 = db.newVertex(vClass);
    v1.setProperty("name", "v1");
    v1.save();

    OVertex v2 = db.newVertex(vClass);
    v2.setProperty("name", "v2");
    v2.save();

    OTodoResultSet createREs = db.command("create edge " + eClass + " from " + v1.getIdentity() + " to " + v2.getIdentity());
    ExecutionPlanPrintUtils.printExecutionPlan(createREs);
    OTodoResultSet result = db.query("select expand(out()) from " + v1.getIdentity());
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals("v2", next.getProperty("name"));
    result.close();

    result = db.query("select expand(in()) from " + v2.getIdentity());
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals("v1", next.getProperty("name"));
    result.close();
  }

  @Test public void testCreateEdgeWithProperty() {
    OSchema schema = db.getMetadata().getSchema();

    String vClass = "testCreateEdgeWithPropertyV";
    schema.createClass(vClass, schema.getClass("V"));

    String eClass = "testCreateEdgeWithPropertyE";
    schema.createClass(eClass, schema.getClass("E"));

    OVertex v1 = db.newVertex(vClass);
    v1.setProperty("name", "v1");
    v1.save();

    OVertex v2 = db.newVertex(vClass);
    v2.setProperty("name", "v2");
    v2.save();

    OTodoResultSet createREs = db
        .command("create edge " + eClass + " from " + v1.getIdentity() + " to " + v2.getIdentity() + " set name = 'theEdge'");
    ExecutionPlanPrintUtils.printExecutionPlan(createREs);
    OTodoResultSet result = db.query("select expand(outE()) from " + v1.getIdentity());
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals("theEdge", next.getProperty("name"));
    result.close();

  }

  @Test public void testCreateTwoByTwo() {
    OSchema schema = db.getMetadata().getSchema();

    String vClass = "testCreateTwoByTwoV";
    schema.createClass(vClass, schema.getClass("V"));

    String eClass = "testCreateTwoByTwoE";
    schema.createClass(eClass, schema.getClass("E"));

    for (int i = 0; i < 4; i++) {
      OVertex v1 = db.newVertex(vClass);
      v1.setProperty("name", "v" + i);
      v1.save();
    }

    OTodoResultSet createREs = db
        .command("create edge " + eClass + " from (select from "+vClass+" where name in ['v0', 'v1']) to  (select from "+vClass+" where name in ['v2', 'v3'])");
    ExecutionPlanPrintUtils.printExecutionPlan(createREs);

    OTodoResultSet result = db.query("select expand(out()) from " + vClass+ " where name = 'v0'");

    Assert.assertNotNull(result);
    for(int i=0;i<2;i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    result.close();

    result = db.query("select expand(in()) from " + vClass+ " where name = 'v2'");

    Assert.assertNotNull(result);
    for(int i=0;i<2;i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    result.close();
  }

}
