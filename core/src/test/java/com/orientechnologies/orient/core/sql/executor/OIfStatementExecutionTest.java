package com.orientechnologies.orient.core.sql.executor;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.BaseMemoryDatabase;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OIfStatementExecutionTest extends BaseMemoryDatabase {

  @Test
  public void testPositive() {
    OResultSet results = db.command("if(1=1){ select 1 as a; }");
    Assert.assertTrue(results.hasNext());
    OResult result = results.next();
    assertThat((Integer) result.getProperty("a")).isEqualTo(1);
    Assert.assertFalse(results.hasNext());
    results.close();
  }

  @Test
  public void testNegative() {
    OResultSet results = db.command("if(1=2){ select 1 as a; }");
    Assert.assertFalse(results.hasNext());
    results.close();
  }

  @Test
  public void testIfReturn() {
    OResultSet results = db.command("if(1=1){ return 'yes'; }");
    Assert.assertTrue(results.hasNext());
    Assert.assertEquals("yes", results.next().getProperty("value"));
    Assert.assertFalse(results.hasNext());
    results.close();
  }
}
