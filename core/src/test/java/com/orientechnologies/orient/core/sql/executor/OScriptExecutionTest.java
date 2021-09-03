package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import java.math.BigDecimal;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OScriptExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OScriptExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.drop();
  }

  @Test
  public void testTwoInserts() {
    String className = "testTwoInserts";
    db.createClass(className);
    db.execute(
        "SQL",
        "INSERT INTO "
            + className
            + " SET name = 'foo';INSERT INTO "
            + className
            + " SET name = 'bar';");
    OResultSet rs = db.query("SELECT count(*) as count from " + className);
    Assert.assertEquals((Object) 2L, rs.next().getProperty("count"));
  }

  @Test
  public void testIf() {
    String className = "testIf";
    db.createClass(className);
    String script = "";
    script += "INSERT INTO " + className + " SET name = 'foo';";
    script += "LET $1 = SELECT count(*) as count FROM " + className + " WHERE name ='bar';";
    script += "IF($1.size() = 0 OR $1[0].count = 0){";
    script += "   INSERT INTO " + className + " SET name = 'bar';";
    script += "}";
    script += "LET $2 = SELECT count(*) as count FROM " + className + " WHERE name ='bar';";
    script += "IF($2.size() = 0 OR $2[0].count = 0){";
    script += "   INSERT INTO " + className + " SET name = 'bar';";
    script += "}";
    db.execute("SQL", script);
    OResultSet rs = db.query("SELECT count(*) as count from " + className);
    Assert.assertEquals((Object) 2L, rs.next().getProperty("count"));
  }

  @Test
  public void testReturnInIf() {
    String className = "testReturnInIf";
    db.createClass(className);
    String script = "";
    script += "INSERT INTO " + className + " SET name = 'foo';";
    script += "LET $1 = SELECT count(*) as count FROM " + className + " WHERE name ='foo';";
    script += "IF($1.size() = 0 OR $1[0].count = 0){";
    script += "   INSERT INTO " + className + " SET name = 'bar';";
    script += "   RETURN;";
    script += "}";
    script += "INSERT INTO " + className + " SET name = 'baz';";
    db.execute("SQL", script);
    OResultSet rs = db.query("SELECT count(*) as count from " + className);
    Assert.assertEquals((Object) 2L, rs.next().getProperty("count"));
  }

  @Test
  public void testReturnInIf2() {
    String className = "testReturnInIf2";
    db.createClass(className);
    String script = "";
    script += "INSERT INTO " + className + " SET name = 'foo';";
    script += "LET $1 = SELECT count(*) as count FROM " + className + " WHERE name ='foo';";
    script += "IF($1.size() > 0 ){";
    script += "   RETURN 'OK';";
    script += "}";
    script += "RETURN 'FAIL';";
    OResultSet result = db.execute("SQL", script);

    OResult item = result.next();

    Assert.assertEquals("OK", item.getProperty("value"));
    result.close();
  }

  @Test
  public void testReturnInIf3() {
    String className = "testReturnInIf3";
    db.createClass(className);
    String script = "";
    script += "INSERT INTO " + className + " SET name = 'foo';";
    script += "LET $1 = SELECT count(*) as count FROM " + className + " WHERE name ='foo';";
    script += "IF($1.size() = 0 ){";
    script += "   RETURN 'FAIL';";
    script += "}";
    script += "RETURN 'OK';";
    OResultSet result = db.execute("SQL", script);

    OResult item = result.next();

    Assert.assertEquals("OK", item.getProperty("value"));
    result.close();
  }

  @Test
  public void testLazyExecutionPlanning() {
    String script = "";
    script +=
        "LET $1 = SELECT FROM (select expand(classes) from metadata:schema) where name = 'nonExistingClass';";
    script += "IF($1.size() > 0) {";
    script += "   SELECT FROM nonExistingClass;";
    script += "   RETURN 'FAIL';";
    script += "}";
    script += "RETURN 'OK';";
    OResultSet result = db.execute("SQL", script);

    OResult item = result.next();

    Assert.assertEquals("OK", item.getProperty("value"));
    result.close();
  }

  @Test
  public void testCommitRetry() {
    String className = "testCommitRetry";
    db.createClass(className);
    String script = "";
    script += "LET $retries = 0;";
    script += "BEGIN;";
    script += "INSERT INTO " + className + " set attempt = $retries;";
    script += "LET $retries = $retries + 1;";
    script += "IF($retries < 5) {";
    script += "  SELECT throwCME(#-1:-1, 1, 1, 1);";
    script += "}";
    script += "COMMIT RETRY 10;";
    db.execute("SQL", script);

    OResultSet result = db.query("select from " + className);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertEquals(4, (int) item.getProperty("attempt"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testCommitRetryWithFailure() {
    String className = "testCommitRetryWithFailure";
    db.createClass(className);
    String script = "";
    script += "LET $retries = 0;";
    script += "BEGIN;";
    script += "INSERT INTO " + className + " set attempt = $retries;";
    script += "LET $retries = $retries + 1;";
    script += "SELECT throwCME(#-1:-1, 1, 1, 1);";
    script += "COMMIT RETRY 10;";
    try {
      db.execute("SQL", script);
    } catch (OConcurrentModificationException x) {
    }

    OResultSet result = db.query("select from " + className);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testCommitRetryWithFailureAndContinue() {
    String className = "testCommitRetryWithFailureAndContinue";
    db.createClass(className);
    String script = "";
    script += "LET $retries = 0;";
    script += "BEGIN;";
    script += "INSERT INTO " + className + " set attempt = $retries;";
    script += "LET $retries = $retries + 1;";
    script += "SELECT throwCME(#-1:-1, 1, 1, 1);";
    script += "COMMIT RETRY 10 ELSE CONTINUE;";
    script += "INSERT INTO " + className + " set name = 'foo';";

    db.execute("SQL", script);

    OResultSet result = db.query("select from " + className);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertEquals("foo", item.getProperty("name"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testCommitRetryWithFailureScriptAndContinue() {
    String className = "testCommitRetryWithFailureScriptAndContinue";
    db.createClass(className);
    String script = "";
    script += "LET $retries = 0;";
    script += "BEGIN;";
    script += "INSERT INTO " + className + " set attempt = $retries;";
    script += "LET $retries = $retries + 1;";
    script += "SELECT throwCME(#-1:-1, 1, 1, 1);";
    script += "COMMIT RETRY 10 ELSE {";
    script += "INSERT INTO " + className + " set name = 'foo';";
    script += "} AND CONTINUE;";

    db.execute("SQL", script);

    OResultSet result = db.query("select from " + className);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertEquals("foo", item.getProperty("name"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testCommitRetryWithFailureScriptAndFail() {
    String className = "testCommitRetryWithFailureScriptAndFail";
    db.createClass(className);
    String script = "";
    script += "LET $retries = 0;";
    script += "BEGIN;";
    script += "INSERT INTO " + className + " set attempt = $retries;";
    script += "LET $retries = $retries + 1;";
    script += "SELECT throwCME(#-1:-1, 1, 1, 1);";
    script += "COMMIT RETRY 10 ELSE {";
    script += "INSERT INTO " + className + " set name = 'foo';";
    script += "} AND FAIL;";

    try {
      db.execute("SQL", script);
      Assert.fail();
    } catch (OConcurrentModificationException e) {

    }

    OResultSet result = db.query("select from " + className);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertEquals("foo", item.getProperty("name"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testCommitRetryWithFailureScriptAndFail2() {
    String className = "testCommitRetryWithFailureScriptAndFail2";
    db.createClass(className);
    String script = "";
    script += "LET $retries = 0;";
    script += "BEGIN;";
    script += "INSERT INTO " + className + " set attempt = $retries;";
    script += "LET $retries = $retries + 1;";
    script += "SELECT throwCME(#-1:-1, 1, 1, 1);";
    script += "COMMIT RETRY 10 ELSE {";
    script += "INSERT INTO " + className + " set name = 'foo';";
    script += "}";

    try {
      db.execute("SQL", script);
      Assert.fail();
    } catch (OConcurrentModificationException e) {

    }

    OResultSet result = db.query("select from " + className);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertEquals("foo", item.getProperty("name"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFunctionAsStatement() {
    String script = "";
    script += "decimal('10');";

    try {
      db.command(script);
      Assert.fail();
    } catch (OCommandSQLParsingException e) {

    }

    OResultSet rs = db.execute("SQL", script);
    Assert.assertTrue(rs.hasNext());
    OResult item = rs.next();
    Assert.assertTrue(item.getProperty("result") instanceof BigDecimal);
    Assert.assertFalse(rs.hasNext());

    rs.close();
  }

  @Test
  public void testAssignOnEdgeCreate() {
    String script = "";
    script += "create class IndirectEdge if not exists extends E;\n";

    script += "insert into V set name = 'a', PrimaryName = 'foo1';\n";
    script += "insert into V set name = 'b', PrimaryName = 'foo2';\n";
    script += "insert into V set name = 'c', PrimaryName = 'foo3';\n";
    script += "insert into V set name = 'd', PrimaryName = 'foo4';\n";

    script +=
        "create edge E from (select from V where name = 'a') to (select from V where name = 'b');\n";
    script +=
        "create edge E from (select from V where name = 'c') to (select from V where name = 'd');\n";

    script += "begin;\n";
    script += "LET SourceDataset = SELECT expand(out()) from V where name = 'a';\n";
    script += "LET TarDataset = SELECT expand(out()) from V where name = 'c';\n";
    script += "IF ($SourceDataset[0] != $TarDataset[0])\n";
    script += "{\n";
    script +=
        "CREATE EDGE IndirectEdge FROM $SourceDataset To $TarDataset SET Source = $SourceDataset[0].PrimaryName;\n";
    script += "};\n";
    script += "commit retry 10;\n";

    db.execute("sql", script).close();

    try (OResultSet rs = db.query("select from IndirectEdge")) {
      Assert.assertEquals("foo2", rs.next().getProperty("Source"));
      Assert.assertFalse(rs.hasNext());
    }
  }
}
