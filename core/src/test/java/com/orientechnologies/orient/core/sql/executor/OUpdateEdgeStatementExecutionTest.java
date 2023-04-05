package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OUpdateEdgeStatementExecutionTest extends BaseMemoryDatabase {

  @Test
  public void testUpdateEdge() {

    db.command("create class V1 extends V");

    db.command("create class E1 extends E");

    db.getMetadata().getSchema().reload();

    // VERTEXES

    OElement v1;
    try (OResultSet res1 = db.command("create vertex")) {
      OResult r = res1.next();
      Assert.assertEquals(r.getProperty("@class"), "V");
      v1 = r.getElement().get();
    }
    OElement v2;
    try (OResultSet res2 = db.command("create vertex V1")) {
      OResult r = res2.next();
      Assert.assertEquals(r.getProperty("@class"), "V1");
      v2 = r.getElement().get();
    }

    OElement v3;
    try (OResultSet res3 = db.command("create vertex set vid = 'v3', brand = 'fiat'")) {
      OResult r = res3.next();
      Assert.assertEquals(r.getProperty("@class"), "V");
      Assert.assertEquals(r.getProperty("brand"), "fiat");
      v3 = r.getElement().get();
    }
    OElement v4;
    try (OResultSet res4 =
        db.command("create vertex V1 set vid = 'v4',  brand = 'fiat',name = 'wow'")) {
      OResult r = res4.next();
      Assert.assertEquals(r.getProperty("@class"), "V1");
      Assert.assertEquals(r.getProperty("brand"), "fiat");
      Assert.assertEquals(r.getProperty("name"), "wow");
      v4 = r.getElement().get();
    }

    OResultSet edges =
        db.command("create edge E1 from " + v1.getIdentity() + " to " + v2.getIdentity());
    Assert.assertTrue(edges.hasNext());
    OResult edge = edges.next();
    Assert.assertFalse(edges.hasNext());
    Assert.assertEquals(((ODocument) edge.getElement().get().getRecord()).getClassName(), "E1");
    edges.close();

    db.command(
        "update edge E1 set out = "
            + v3.getIdentity()
            + ", in = "
            + v4.getIdentity()
            + " where @rid = "
            + edge.getElement().get().getIdentity());

    OResultSet result = db.query("select expand(out('E1')) from " + v3.getIdentity());
    Assert.assertTrue(result.hasNext());
    OResult vertex4 = result.next();
    Assert.assertEquals(vertex4.getProperty("vid"), "v4");
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select expand(in('E1')) from " + v4.getIdentity());
    Assert.assertTrue(result.hasNext());
    OResult vertex3 = result.next();
    Assert.assertEquals(vertex3.getProperty("vid"), "v3");
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select expand(out('E1')) from " + v1.getIdentity());
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select expand(in('E1')) from " + v2.getIdentity());
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testUpdateEdgeOfTypeE() {
    // issue #6378

    OVertex v1 = db.newVertex();
    db.save(v1);
    OVertex v2 = db.newVertex();
    db.save(v2);
    OVertex v3 = db.newVertex();
    db.save(v3);

    OResultSet edges =
        db.command("create edge E from " + v1.getIdentity() + " to " + v2.getIdentity());
    OResult edge = edges.next();

    db.command(
            "UPDATE EDGE "
                + edge.getElement().get().getIdentity()
                + " SET in = "
                + v3.getIdentity())
        .close();
    edges.close();

    OResultSet result = db.query("select expand(out()) from " + v1.getIdentity());

    Assert.assertEquals(result.next().getIdentity().get(), v3.getIdentity());
    result.close();

    result = db.query("select expand(in()) from " + v3.getIdentity());
    Assert.assertEquals(result.next().getIdentity().get(), v1.getIdentity());
    result.close();

    result = db.command("select expand(in()) from " + v2.getIdentity());
    Assert.assertFalse(result.hasNext());
    result.close();
  }
}
