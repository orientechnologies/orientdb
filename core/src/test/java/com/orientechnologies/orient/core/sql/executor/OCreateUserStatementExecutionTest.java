package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OCreateUserStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OCreateUserStatementExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  @Test
  public void testPlain() {
    String name = "testPlain";
    OResultSet result = db.command("CREATE USER test IDENTIFIED BY foo ROLE admin");
    result.close();

    result = db.query("SELECT name, roles.name as roles FROM OUser WHERE name = 'test'");
    Assert.assertTrue(result.hasNext());
    OResult user = result.next();
    Assert.assertEquals("test", user.getProperty("name"));
    List<String> roles = user.getProperty("roles");
    Assert.assertEquals(1, roles.size());
    Assert.assertEquals("admin", roles.get(0));

    result.close();
  }
}
