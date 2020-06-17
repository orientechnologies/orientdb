package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class ODropClusterStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:ODropClusterStatementExecutionTest");
    db.create();
    OClass v = db.getMetadata().getSchema().getClass("V");
    if (v == null) {
      db.getMetadata().getSchema().createClass("V");
    }
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  @Test
  public void testPlain() {
    String cluster = "testPlain";
    ((ODatabaseDocumentTx) db).getStorage().addCluster(cluster);

    Assert.assertTrue(db.getClusterIdByName(cluster) > 0);
    OResultSet result = db.command("drop cluster " + cluster);
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertEquals("drop cluster", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();

    Assert.assertTrue(db.getClusterIdByName(cluster) < 0);
  }

  @Test
  public void testDropClusterIfExists() {
    String cluster = "testDropClusterIfExists";
    ((ODatabaseDocumentTx) db).getStorage().addCluster(cluster);

    Assert.assertTrue(db.getClusterIdByName(cluster) > 0);
    OResultSet result = db.command("drop cluster " + cluster + " IF EXISTS");
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertEquals("drop cluster", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();

    Assert.assertTrue(db.getClusterIdByName(cluster) < 0);

    result = db.command("drop cluster " + cluster + " IF EXISTS");
    Assert.assertFalse(result.hasNext());
    result.close();
  }
}
