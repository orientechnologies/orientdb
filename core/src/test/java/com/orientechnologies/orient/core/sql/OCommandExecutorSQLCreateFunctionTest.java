package com.orientechnologies.orient.core.sql;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OResultSet;

/**
 * @author Luigi Dell'Aquila
 */
@Test
public class OCommandExecutorSQLCreateFunctionTest {

  private static String DB_STORAGE = "memory";
  private static String DB_NAME    = "OCommandExecutorSQLCreateFunctionTest";

  ODatabaseDocumentTx   db;

  @BeforeClass
  public void beforeClass() throws Exception {
    db = new ODatabaseDocumentTx(DB_STORAGE + ":" + DB_NAME);
    db.create();

  }

  @AfterClass
  public void afterClass() throws Exception {
    if (db.isClosed()) {
      db.open("admin", "admin");
    }
    db.close();
  }

  @Test
  public void testCreateFunction() {
    db.command(
        new OCommandSQL(
            "CREATE FUNCTION testCreateFunction {return 'hello '+name;} PARAMETERS [name] IDEMPOTENT true LANGUAGE Javascript"))
        .execute();
    OResultSet<ODocument> result = db.command(new OCommandSQL("select testCreateFunction('world') as name")).execute();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).field("name"), "hello world");

  }
}
