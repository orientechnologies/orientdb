package com.orientechnologies.orient.core.sql.executor;

import static com.orientechnologies.orient.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

import com.orientechnologies.BaseMemoryDatabase;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OCommitStatementExecutionTest extends BaseMemoryDatabase {
  @Test
  public void testBegin() {
    Assert.assertTrue(db.getTransaction() == null || !db.getTransaction().isActive());
    db.begin();
    Assert.assertFalse(db.getTransaction() == null || !db.getTransaction().isActive());
    OResultSet result = db.command("commit");
    printExecutionPlan(null, result);
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertEquals("commit", item.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(db.getTransaction() == null || !db.getTransaction().isActive());
    db.commit();
  }
}
