package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import java.util.Optional;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OExplainStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {

    db = new ODatabaseDocumentTx("memory:OExplainStatementExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

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
