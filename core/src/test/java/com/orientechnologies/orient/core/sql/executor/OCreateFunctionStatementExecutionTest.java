package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OCreateFunctionStatementExecutionTest extends BaseMemoryDatabase {

  @Test
  public void testPlain() {
    String name = "testPlain";
    OResultSet result =
        db.command(
            "CREATE FUNCTION " + name + " \"return a + b;\" PARAMETERS [a,b] language javascript");
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertFalse(result.hasNext());
    Assert.assertNotNull(next);
    Assert.assertEquals(name, next.getProperty("functionName"));
    result.close();

    result = db.query("select " + name + "('foo', 'bar') as sum");
    Assert.assertTrue(result.hasNext());
    next = result.next();
    Assert.assertFalse(result.hasNext());
    Assert.assertNotNull(next);
    Assert.assertEquals("foobar", next.getProperty("sum"));
    result.close();
  }
}
