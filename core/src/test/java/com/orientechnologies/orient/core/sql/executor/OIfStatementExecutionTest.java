package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OIfStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OIfStatementExecutionTest");
    db.create();
  }

  @AfterClass public static void afterClass() {
    db.drop();
  }

  @Test public void testPositive() {
    OTodoResultSet results = db.command("if(1=1){ select 1 as a; }");
    Assert.assertTrue(results.hasNext());
    OResult result = results.next();
    assertThat((Integer) result.getProperty("a")).isEqualTo(1);
    Assert.assertFalse(results.hasNext());
    results.close();
  }

  @Test public void testNegative() {
    OTodoResultSet results = db.command("if(1=2){ select 1 as a; }");
    Assert.assertFalse(results.hasNext());
    results.close();
  }


}
