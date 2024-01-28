package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.OVertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OOptimizeDatabaseExecutionTest extends BaseMemoryDatabase {

  @Test
  public void test() {
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

    db.command("optimize database -LWEDGES").close();

    OResultSet rs = db.query("select from E");
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }
}
