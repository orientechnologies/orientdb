package com.orientechnologies.orient.core.sql.executor;

import static com.orientechnologies.orient.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

import com.orientechnologies.BaseMemoryDatabase;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OSleepStatementExecutionTest extends BaseMemoryDatabase {

  @Test
  public void testBasic() {
    long begin = System.currentTimeMillis();
    OResultSet result = db.command("sleep 1000");
    Assert.assertTrue(System.currentTimeMillis() - begin >= 1000);
    printExecutionPlan(null, result);
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertEquals("sleep", item.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
  }
}
