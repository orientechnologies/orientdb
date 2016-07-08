package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by luigidellaquila on 07/07/16.
 */
public class OSelectStatementExecutionTest {
  static ODatabaseDocumentTx db;

  @BeforeClass
  public static void beforeClass(){

    db = new ODatabaseDocumentTx("memory:OSelectStatementExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass(){
    db.close();
  }


  @Test
  public void testSelectNoTarget(){
    OTodoResultSet result = db.query("select 1 as one, 2 as two, 2+3");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(1L, item.getProperty("one"));
    Assert.assertEquals(2L, item.getProperty("two"));
    Assert.assertEquals(5L, item.getProperty("2 + 3"));
  }

  @Test
  public void testSelectNoTargetSkip(){
    OTodoResultSet result = db.query("select 1 as one, 2 as two, 2+3 skip 1");
    Assert.assertFalse(result.hasNext());
  }

  @Test
  public void testSelectNoTargetSkipZero(){
    OTodoResultSet result = db.query("select 1 as one, 2 as two, 2+3 skip 0");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(1L, item.getProperty("one"));
    Assert.assertEquals(2L, item.getProperty("two"));
    Assert.assertEquals(5L, item.getProperty("2 + 3"));
  }
}
