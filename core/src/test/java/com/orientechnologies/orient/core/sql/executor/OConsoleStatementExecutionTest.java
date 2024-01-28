package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OConsoleStatementExecutionTest extends BaseMemoryDatabase {

  @Test
  public void testError() {
    OResultSet result = db.command("console.error 'foo bar'");
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals("error", item.getProperty("level"));
    Assert.assertEquals("foo bar", item.getProperty("message"));
  }

  @Test
  public void testLog() {
    OResultSet result = db.command("console.log 'foo bar'");
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals("log", item.getProperty("level"));
    Assert.assertEquals("foo bar", item.getProperty("message"));
  }

  @Test
  public void testInvalidLevel() {
    try {
      db.command("console.bla 'foo bar'");
      Assert.fail();
    } catch (OCommandExecutionException x) {

    } catch (Exception x2) {
      Assert.fail();
    }
  }
}
