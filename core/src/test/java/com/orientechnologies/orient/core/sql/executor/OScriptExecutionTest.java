package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
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
    db.execute("SQL", "INSERT INTO " + className + " SET name = 'foo';INSERT INTO " + className + " SET name = 'bar';");
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
    script += "LET $1 = SELECT FROM (select expand(classes) from metadata:schema) where name = 'nonExistingClass';";
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
}
