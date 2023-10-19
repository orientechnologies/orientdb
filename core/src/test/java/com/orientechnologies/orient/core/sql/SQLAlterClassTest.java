package com.orientechnologies.orient.core.sql;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class SQLAlterClassTest extends BaseMemoryDatabase {

  @Test
  public void alterClassRenameTest() {
    db.getMetadata().getSchema().createClass("TestClass");

    try {
      db.command(new OCommandSQL("alter class TestClass name = 'test_class'")).execute();
      Assert.fail("the rename should fail for wrong syntax");
    } catch (OCommandSQLParsingException ex) {

    }
    Assert.assertNotNull(db.getMetadata().getSchema().getClass("TestClass"));
  }

  @Test
  public void testQuoted() {
    try {
      db.command(new OCommandSQL("create class `Client-Type`")).execute();
      db.command(new OCommandSQL("alter class `Client-Type` addcluster `client-type_usa`"))
          .execute();
      db.command(new OCommandSQL("insert into `Client-Type` set foo = 'bar'")).execute();
      List<?> result = db.query(new OSQLSynchQuery<Object>("Select from `Client-Type`"));
      Assert.assertEquals(result.size(), 1);
    } catch (OCommandSQLParsingException ex) {
      Assert.fail();
    }
  }
}
