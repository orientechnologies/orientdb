package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OExplainStatementExecutionTest extends BaseMemoryDatabase {

  @Test
  public void testExplainSelectNoTarget() {
    OResultSet result = db.query("explain select 1 as one, 2 as two, 2+3");
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertNotNull(next.getProperty("executionPlan"));
    Assert.assertNotNull(next.getProperty("executionPlanAsString"));

    Optional<OExecutionPlan> plan = result.getExecutionPlan();
    Assert.assertTrue(plan.isPresent());
    Assert.assertTrue(plan.get() instanceof OSelectExecutionPlan);

    result.close();
  }
}
