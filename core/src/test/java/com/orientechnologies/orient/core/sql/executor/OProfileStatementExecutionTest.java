package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OProfileStatementExecutionTest extends BaseMemoryDatabase {

  @Test
  public void testProfile() {
    db.createClass("testProfile");
    db.command("insert into testProfile set name ='foo'");
    db.command("insert into testProfile set name ='bar'");

    OResultSet result = db.query("PROFILE SELECT FROM testProfile WHERE name ='bar'");
    Assert.assertTrue(result.getExecutionPlan().get().prettyPrint(0, 2).contains("μs"));

    result.close();
  }
}
