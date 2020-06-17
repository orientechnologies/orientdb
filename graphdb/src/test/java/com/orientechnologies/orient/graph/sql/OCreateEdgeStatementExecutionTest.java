package com.orientechnologies.orient.graph.sql;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.ExecutionPlanPrintUtils;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OCreateEdgeStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
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

  @AfterClass
  public static void afterClass() {
    db.drop();
  }

  @Test
  public void testCreateSingleEdge() {
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

    OResultSet createREs =
        db.command(
            "create edge " + eClass + " from " + v1.getIdentity() + " to " + v2.getIdentity());
    ExecutionPlanPrintUtils.printExecutionPlan(createREs);
    OResultSet result = db.query("select expand(out()) from " + v1.getIdentity());
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

  @Test
  public void testCreateEdgeWithProperty() {
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

    OResultSet createREs =
        db.command(
            "create edge "
                + eClass
                + " from "
                + v1.getIdentity()
                + " to "
                + v2.getIdentity()
                + " set name = 'theEdge'");
    ExecutionPlanPrintUtils.printExecutionPlan(createREs);
    OResultSet result = db.query("select expand(outE()) from " + v1.getIdentity());
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals("theEdge", next.getProperty("name"));
    result.close();
  }

  @Test
  public void testCreateTwoByTwo() {
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

    OResultSet createREs =
        db.command(
            "create edge "
                + eClass
                + " from (select from "
                + vClass
                + " where name in ['v0', 'v1']) to  (select from "
                + vClass
                + " where name in ['v2', 'v3'])");
    ExecutionPlanPrintUtils.printExecutionPlan(createREs);

    OResultSet result = db.query("select expand(out()) from " + vClass + " where name = 'v0'");

    Assert.assertNotNull(result);
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    result.close();

    result = db.query("select expand(in()) from " + vClass + " where name = 'v2'");

    Assert.assertNotNull(result);
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    result.close();
  }

  @Test
  public void testUpsert() {
    OSchema schema = db.getMetadata().getSchema();

    String vClass1 = "testUpsertV1";
    OClass vclazz1 = schema.createClass(vClass1, schema.getClass("V"));

    String vClass2 = "testUpsertV2";
    OClass vclazz2 = schema.createClass(vClass2, schema.getClass("V"));

    String eClass = "testUpsertE";

    OClass eclazz = schema.createClass(eClass, schema.getClass("E"));
    eclazz.createProperty("out", OType.LINK, vclazz1);
    eclazz.createProperty("in", OType.LINK, vclazz2);

    db.command("CREATE INDEX " + eClass + "out_in ON " + eclazz + " (out, in) UNIQUE");

    for (int i = 0; i < 2; i++) {
      OVertex v1 = db.newVertex(vClass1);
      v1.setProperty("name", "v" + i);
      v1.save();
    }

    for (int i = 0; i < 2; i++) {
      OVertex v1 = db.newVertex(vClass2);
      v1.setProperty("name", "v" + i);
      v1.save();
    }

    db.command(
            "CREATE EDGE "
                + eClass
                + " from (select from "
                + vClass1
                + " where name = 'v0') to  (select from "
                + vClass2
                + " where name = 'v0') SET name = 'foo'")
        .close();

    OResultSet rs = db.query("SELECT FROM " + eClass);
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();

    db.command(
            "CREATE EDGE "
                + eClass
                + " UPSERT from (select from "
                + vClass1
                + ") to  (select from "
                + vClass2
                + ") SET name = 'bar'")
        .close();

    rs = db.query("SELECT FROM " + eclazz);
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(rs.hasNext());
      OResult item = rs.next();
      Assert.assertEquals("bar", item.getProperty("name"));
    }
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testUpsertHashIndex() {
    OSchema schema = db.getMetadata().getSchema();

    String vClass1 = "testUpsertHashIndexV1";
    OClass vclazz1 = schema.createClass(vClass1, schema.getClass("V"));

    String vClass2 = "testUpsertHashIndexV2";
    OClass vclazz2 = schema.createClass(vClass2, schema.getClass("V"));

    String eClass = "testUpsertHashIndexE";

    OClass eclazz = schema.createClass(eClass, schema.getClass("E"));
    eclazz.createProperty("out", OType.LINK, vclazz1);
    eclazz.createProperty("in", OType.LINK, vclazz2);

    db.command("CREATE INDEX " + eClass + "out_in ON " + eclazz + " (out, in) UNIQUE");

    for (int i = 0; i < 2; i++) {
      OVertex v1 = db.newVertex(vClass1);
      v1.setProperty("name", "v" + i);
      v1.save();
    }

    for (int i = 0; i < 2; i++) {
      OVertex v1 = db.newVertex(vClass2);
      v1.setProperty("name", "v" + i);
      v1.save();
    }

    db.command(
            "CREATE EDGE "
                + eClass
                + " from (select from "
                + vClass1
                + " where name = 'v0') to  (select from "
                + vClass2
                + " where name = 'v0')")
        .close();

    OResultSet rs = db.query("SELECT FROM " + eClass);
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();

    db.command(
            "CREATE EDGE "
                + eClass
                + " UPSERT from (select from "
                + vClass1
                + ") to  (select from "
                + vClass2
                + ")")
        .close();

    rs = db.query("SELECT FROM " + eclazz);
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(rs.hasNext());
      rs.next();
    }
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testBreakUniqueWithoutUpsert() {
    OSchema schema = db.getMetadata().getSchema();

    String vClass1 = "testBreakUniqueWithoutUpsertV1";
    OClass vclazz1 = schema.createClass(vClass1, schema.getClass("V"));

    String vClass2 = "testBreakUniqueWithoutUpsertV2";
    OClass vclazz2 = schema.createClass(vClass2, schema.getClass("V"));

    String eClass = "testBreakUniqueWithoutUpsertE";

    OClass eclazz = schema.createClass(eClass, schema.getClass("E"));
    eclazz.createProperty("out", OType.LINK, vclazz1);
    eclazz.createProperty("in", OType.LINK, vclazz2);

    db.command("CREATE INDEX " + eClass + "out_in ON " + eclazz + " (out, in) UNIQUE");

    for (int i = 0; i < 2; i++) {
      OVertex v1 = db.newVertex(vClass1);
      v1.setProperty("name", "v" + i);
      v1.save();
    }

    for (int i = 0; i < 2; i++) {
      OVertex v1 = db.newVertex(vClass2);
      v1.setProperty("name", "v" + i);
      v1.save();
    }

    db.command(
            "CREATE EDGE "
                + eClass
                + " from (select from "
                + vClass1
                + " where name = 'v0') to  (select from "
                + vClass2
                + " where name = 'v0')")
        .close();

    OResultSet rs = db.query("SELECT FROM " + eClass);
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();

    try {
      db.command(
              "CREATE EDGE "
                  + eClass
                  + " from (select from "
                  + vClass1
                  + ") to  (select from "
                  + vClass2
                  + ")")
          .close();
      Assert.fail();
    } catch (ORecordDuplicatedException | OCommandExecutionException e) {

    }
  }

  @Test
  public void testUpsertNoIndex() {
    OSchema schema = db.getMetadata().getSchema();

    String vClass1 = "testUpsertNoIndexV1";
    OClass vclazz1 = schema.createClass(vClass1, schema.getClass("V"));

    String vClass2 = "testUpsertNoIndexV2";
    OClass vclazz2 = schema.createClass(vClass2, schema.getClass("V"));

    String eClass = "testUpsertNoIndexE";

    for (int i = 0; i < 2; i++) {
      OVertex v1 = db.newVertex(vClass1);
      v1.setProperty("name", "v" + i);
      v1.save();
    }

    for (int i = 0; i < 2; i++) {
      OVertex v1 = db.newVertex(vClass2);
      v1.setProperty("name", "v" + i);
      v1.save();
    }

    try {
      db.command(
              "CREATE EDGE "
                  + eClass
                  + " UPSERT from (select from "
                  + vClass1
                  + ") to  (select from "
                  + vClass2
                  + ")")
          .close();
      Assert.fail();
    } catch (OCommandExecutionException e) {

    }
  }

  @Test
  public void testPositionalParams() {

    String vClass1 = "testPositionalParamsV";
    db.createVertexClass(vClass1);

    String eClass = "testPositionalParamsE";
    db.createEdgeClass(eClass);

    for (int i = 0; i < 2; i++) {
      OVertex v1 = db.newVertex(vClass1);
      v1.setProperty("name", "v" + i);
      v1.save();
    }

    db.command(
            "CREATE EDGE "
                + eClass
                + " from (select from "
                + vClass1
                + " WHERE name = ? ) to  (select from "
                + vClass1
                + " WHERE name = ? )",
            "v0",
            "v1")
        .close();

    OResultSet result =
        db.query("select from " + eClass + " where out.name = 'v0' AND in.name = 'v1'");
    Assert.assertTrue(result.hasNext());
    result.close();
  }
}
