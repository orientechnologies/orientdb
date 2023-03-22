package com.orientechnologies.orient.core.sql;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OLegacyResultSet;
import org.junit.Assert;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OCommandExecutorSQLCreateFunctionTest extends BaseMemoryDatabase {

  @Test
  public void testCreateFunction() {
    db.command(
            "CREATE FUNCTION testCreateFunction \"return 'hello '+name;\" PARAMETERS [name] IDEMPOTENT true LANGUAGE Javascript")
        .close();
    OLegacyResultSet<ODocument> result =
        db.command(new OCommandSQL("select testCreateFunction('world') as name")).execute();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).field("name"), "hello world");
  }
}
