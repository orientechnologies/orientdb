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
public class ODeleteEdgeStatementExecutionTest {
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
  public void testDeleteSingleEdge() {
    String vertexClassName = "testDeleteSingleEdgeV";
    db.createVertexClass(vertexClassName);

    String edgeClassName = "testDeleteSingleEdgeE";
    db.createEdgeClass(edgeClassName);

    OVertex prev = null;
    for (int i = 0; i < 10; i++) {
      OVertex v1 = db.newVertex(vertexClassName);
      v1.setProperty("name", "a" + i);
      v1.save();
      if (prev != null) {
        prev.addEdge(v1, edgeClassName).save();
      }
      prev = v1;
    }

    OResultSet rs = db.query("SELECT expand(out()) FROM " + vertexClassName);
    Assert.assertEquals(9, rs.stream().count());
    rs.close();

    rs = db.query("SELECT expand(in()) FROM " + vertexClassName);
    Assert.assertEquals(9, rs.stream().count());
    rs.close();

    db.command(
            "DELETE EDGE "
                + edgeClassName
                + " from (SELECT FROM "
                + vertexClassName
                + " where name = 'a1') to (SELECT FROM "
                + vertexClassName
                + " where name = 'a2')")
        .close();

    rs = db.query("SELECT FROM " + edgeClassName);
    Assert.assertEquals(8, rs.stream().count());
    rs.close();

    rs = db.query("SELECT expand(out()) FROM " + vertexClassName + " where name = 'a1'");
    Assert.assertEquals(0, rs.stream().count());
    rs.close();

    rs = db.query("SELECT expand(in()) FROM " + vertexClassName + " where name = 'a2'");
    Assert.assertEquals(0, rs.stream().count());
    rs.close();
  }

  @Test
  public void testDeleteAll() {
    String vertexClassName = "testDeleteAllV";
    db.createVertexClass(vertexClassName);

    String edgeClassName = "testDeleteAllE";
    db.createEdgeClass(edgeClassName);

    OVertex prev = null;
    for (int i = 0; i < 10; i++) {
      OVertex v1 = db.newVertex(vertexClassName);
      v1.setProperty("name", "a" + i);
      v1.save();
      if (prev != null) {
        prev.addEdge(v1, edgeClassName).save();
      }
      prev = v1;
    }

    OResultSet rs = db.query("SELECT expand(out()) FROM " + vertexClassName);
    Assert.assertEquals(9, rs.stream().count());
    rs.close();

    rs = db.query("SELECT expand(in()) FROM " + vertexClassName);
    Assert.assertEquals(9, rs.stream().count());
    rs.close();

    db.command("DELETE EDGE " + edgeClassName).close();

    rs = db.query("SELECT FROM " + edgeClassName);
    Assert.assertEquals(0, rs.stream().count());
    rs.close();

    rs = db.query("SELECT expand(out()) FROM " + vertexClassName);
    Assert.assertEquals(0, rs.stream().count());
    rs.close();

    rs = db.query("SELECT expand(in()) FROM " + vertexClassName);
    Assert.assertEquals(0, rs.stream().count());
    rs.close();
  }
}
