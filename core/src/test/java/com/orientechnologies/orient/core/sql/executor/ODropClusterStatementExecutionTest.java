package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class ODropClusterStatementExecutionTest extends BaseMemoryDatabase {

  @Test
  public void testPlain() {
    String cluster = "testPlain";
    ((ODatabaseDocumentInternal) db).getStorage().addCluster(cluster);

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
    ((ODatabaseDocumentInternal) db).getStorage().addCluster(cluster);

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
