package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OCreateClusterStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OCreateClusterStatementExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  @Test
  public void testPlain() {
    String clusterName = "testPlain";
    OResultSet result = db.command("create cluster " + clusterName);
    Assert.assertTrue(db.getClusterIdByName(clusterName) > 0);
    result.close();
  }

  @Test
  public void testExisting() {
    OClass clazz = db.getMetadata().getSchema().createClass("testExisting");
    String clusterName = db.getClusterNameById(clazz.getClusterIds()[0]);
    try {
      db.command("create cluster " + clusterName);
      Assert.fail();
    } catch (OCommandExecutionException ex) {

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testWithNumber() {
    String clusterName = "testWithNumber";
    OResultSet result = db.command("create cluster " + clusterName + " id 1000");
    Assert.assertTrue(db.getClusterIdByName(clusterName) > 0);
    Assert.assertNotNull(db.getClusterNameById(1000));

    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertFalse(result.hasNext());
    Assert.assertNotNull(next);
    Assert.assertEquals((Object) 1000, next.getProperty("requestedId"));
    result.close();
  }

  @Test
  public void testBlob() {
    String clusterName = "testBlob";
    OResultSet result = db.command("create blob cluster " + clusterName);
    Assert.assertTrue(db.getClusterIdByName(clusterName) > 0);
    Assert.assertTrue(((ODatabaseDocumentTx) db).getStorage().getClusterIdByName(clusterName) >= 0);
    // TODO test that it's a blob cluster
    result.close();
  }

  @Test
  public void testIfNotExists() {
    String clusterName = "testIfNotExists";
    OResultSet result = db.command("create cluster " + clusterName + " IF NOT EXISTS id 2000");
    Assert.assertTrue(db.getClusterIdByName(clusterName) > 0);
    Assert.assertNotNull(db.getClusterNameById(2000));

    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertFalse(result.hasNext());
    Assert.assertNotNull(next);
    Assert.assertEquals((Object) 2000, next.getProperty("requestedId"));
    result.close();

    result = db.command("create cluster " + clusterName + " IF NOT EXISTS id 1000");
    Assert.assertFalse(result.hasNext());
    result.close();
  }
}
