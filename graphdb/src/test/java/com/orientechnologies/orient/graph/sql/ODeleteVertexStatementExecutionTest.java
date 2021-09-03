package com.orientechnologies.orient.graph.sql;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class ODeleteVertexStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:ODeleteVertexStatementExecutionTest");
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
  public void testDeleteSingleVertex() {
    String className = "testDeleteSingleVertex";
    db.createVertexClass(className);
    for (int i = 0; i < 10; i++) {
      OVertex v1 = db.newVertex(className);
      v1.setProperty("name", "a" + i);
      v1.save();
    }

    db.command("DELETE VERTEX " + className + " WHERE name = 'a3'").close();
    OResultSet rs = db.query("SELECT FROM " + className);
    Assert.assertEquals(9, rs.stream().count());
    rs.close();
  }

  @Test
  public void testDeleteAllVertices() {
    String className = "testDeleteAllVertices";
    db.createVertexClass(className);
    for (int i = 0; i < 10; i++) {
      OVertex v1 = db.newVertex(className);
      v1.setProperty("name", "a" + i);
      v1.save();
    }

    db.command("DELETE VERTEX " + className).close();
    OResultSet rs = db.query("SELECT FROM " + className);
    Assert.assertEquals(0, rs.stream().count());
    rs.close();
  }

  @Test
  public void testFilterClass() {
    String className1 = "testDeleteAllVertices1";
    db.createVertexClass(className1);
    for (int i = 0; i < 10; i++) {
      OVertex v1 = db.newVertex(className1);
      v1.setProperty("name", "a" + i);
      v1.save();
    }

    String className2 = "testDeleteAllVertices2";
    db.createVertexClass(className2);
    for (int i = 0; i < 10; i++) {
      OVertex v1 = db.newVertex(className2);
      v1.setProperty("name", "a" + i);
      v1.save();
    }

    db.command("DELETE VERTEX " + className1).close();
    OResultSet rs = db.query("SELECT FROM " + className1);
    Assert.assertEquals(0, rs.stream().count());
    rs.close();

    rs = db.query("SELECT FROM " + className2);
    Assert.assertEquals(10, rs.stream().count());
    rs.close();
  }
}
