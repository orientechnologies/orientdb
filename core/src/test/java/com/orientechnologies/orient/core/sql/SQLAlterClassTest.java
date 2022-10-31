package com.orientechnologies.orient.core.sql;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Test;

public class SQLAlterClassTest extends BaseMemoryDatabase {

  @Test
  public void alterClassRenameTest() {
    db.getMetadata().getSchema().createClass("TestClass");

    try {
      db.command("alter class TestClass name = 'test_class'").close();
      Assert.fail("the rename should fail for wrong syntax");
    } catch (OCommandSQLParsingException ex) {

    }
    Assert.assertNotNull(db.getMetadata().getSchema().getClass("TestClass"));
  }

  @Test
  public void testQuoted() {
    try {
      db.command("create class `Client-Type`").close();
      db.command("alter class `Client-Type` addcluster `client-type_usa`").close();
      db.command("insert into `Client-Type` set foo = 'bar'").close();
      OResultSet result = db.query("Select from `Client-Type`");
      Assert.assertEquals(result.stream().count(), 1);
    } catch (OCommandSQLParsingException ex) {
      Assert.fail();
    }
  }
}
