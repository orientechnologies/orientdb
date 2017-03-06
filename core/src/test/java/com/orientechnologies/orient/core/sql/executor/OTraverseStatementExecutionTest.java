package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OTraverseStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {

    db = new ODatabaseDocumentTx("memory:OTraverseStatementExecutionTest");
    db.create();

    initBaseGraph(db);
  }

  private static void initBaseGraph(ODatabaseDocument db) {

  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  @Test
  public void testPlainTraverse() {
    String classPrefix = "testPlainTraverse_";
    db.createVertexClass(classPrefix + "V");
    db.createEdgeClass(classPrefix + "E");
    db.command("create vertex " + classPrefix + "V set name = 'a'").close();
    db.command("create vertex " + classPrefix + "V set name = 'b'").close();
    db.command("create vertex " + classPrefix + "V set name = 'c'").close();
    db.command("create vertex " + classPrefix + "V set name = 'd'").close();

    db.command(
        "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'a') to (select from " + classPrefix
            + "V where name = 'b')").close();
    db.command(
        "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'b') to (select from " + classPrefix
            + "V where name = 'c')").close();
    db.command(
        "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'c') to (select from " + classPrefix
            + "V where name = 'd')").close();

    OResultSet result = db.query("traverse out() from (select from " + classPrefix + "V where name = 'a')");

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertEquals(i, item.getMetadata("$depth"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testWithDepth() {
    String classPrefix = "testWithDepth_";
    db.createVertexClass(classPrefix + "V");
    db.createEdgeClass(classPrefix + "E");
    db.command("create vertex " + classPrefix + "V set name = 'a'").close();
    db.command("create vertex " + classPrefix + "V set name = 'b'").close();
    db.command("create vertex " + classPrefix + "V set name = 'c'").close();
    db.command("create vertex " + classPrefix + "V set name = 'd'").close();

    db.command(
        "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'a') to (select from " + classPrefix
            + "V where name = 'b')").close();
    db.command(
        "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'b') to (select from " + classPrefix
            + "V where name = 'c')").close();
    db.command(
        "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'c') to (select from " + classPrefix
            + "V where name = 'd')").close();

    OResultSet result = db.query("traverse out() from (select from " + classPrefix + "V where name = 'a') WHILE $depth < 2");

    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertEquals(i, item.getMetadata("$depth"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

}
