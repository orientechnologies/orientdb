package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.orientechnologies.orient.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

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
  public void testSelectNoTarget() {
    OTodoResultSet result = db.query("select 1 as one, 2 as two, 2+3");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(1, item.<Object>getProperty("one"));
    Assert.assertEquals(2, item.<Object>getProperty("two"));
    Assert.assertEquals(5, item.<Object>getProperty("2 + 3"));
    printExecutionPlan(result);

    result.close();
  }

}
