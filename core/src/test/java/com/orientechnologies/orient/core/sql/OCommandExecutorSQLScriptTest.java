package com.orientechnologies.orient.core.sql;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class OCommandExecutorSQLScriptTest {
  private static String DB_STORAGE = "memory";
  private static String DB_NAME = "OCommandExecutorSQLScriptTest";
  public ODatabaseDocumentTx db;

  @After
  public void after() throws Exception {
    if (db.isClosed()) {
      db.open("admin", "admin");
    }
    db.drop();
  }

  @Before
  public void before() throws Exception {
    db = new ODatabaseDocumentTx(DB_STORAGE + ":" + DB_NAME);
    db.create();

    db.command(new OCommandSQL("CREATE class foo")).execute();

    db.command(new OCommandSQL("insert into foo (name, bar) values ('a', 1)")).execute();
    db.command(new OCommandSQL("insert into foo (name, bar) values ('b', 2)")).execute();
    db.command(new OCommandSQL("insert into foo (name, bar) values ('c', 3)")).execute();

    db.activateOnCurrentThread();
  }

  @Test
  public void testQuery() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("begin\n");
    script.append("let $a = select from foo\n");
    script.append("commit\n");
    script.append("return $a\n");
    List<ODocument> qResult = db.command(new OCommandScript("sql", script.toString())).execute();

    Assert.assertEquals(qResult.size(), 3);
  }

  @Test
  public void testTx() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("begin isolation REPEATABLE_READ\n");
    script.append("let $a = insert into V set test = 'sql script test'\n");
    script.append("commit retry 10\n");
    script.append("return $a\n");
    ODocument qResult = db.command(new OCommandScript("sql", script.toString())).execute();

    Assert.assertNotNull(qResult);
  }

  @Test
  public void testReturnExpanded() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("let $a = insert into V set test = 'sql script test'\n");
    script.append("return $a.toJSON()\n");
    String qResult = db.command(new OCommandScript("sql", script.toString())).execute();
    Assert.assertNotNull(qResult);

    new ODocument().fromJSON(qResult);

    script = new StringBuilder();
    script.append("let $a = select from V limit 2\n");
    script.append("return $a.toJSON()\n");
    String result = db.command(new OCommandScript("sql", script.toString())).execute();

    Assert.assertNotNull(result);
    result = result.trim();
    Assert.assertTrue(result.startsWith("["));
    Assert.assertTrue(result.endsWith("]"));
    new ODocument().fromJSON(result.substring(1, result.length() - 1));
  }

  @Test
  public void testSleep() throws Exception {
    long begin = System.currentTimeMillis();

    StringBuilder script = new StringBuilder();
    script.append("sleep 500");
    db.command(new OCommandScript("sql", script.toString())).execute();

    Assert.assertTrue(System.currentTimeMillis() - begin >= 500);
  }

  @Test
  public void testConsoleLog() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("LET $a = 'log'\n");
    script.append("console.log 'This is a test of log for ${a}'");
    db.command(new OCommandScript("sql", script.toString())).execute();
  }

  @Test
  public void testConsoleOutput() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("LET $a = 'output'\n");
    script.append("console.output 'This is a test of log for ${a}'");
    db.command(new OCommandScript("sql", script.toString())).execute();
  }

  @Test
  public void testConsoleError() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("LET $a = 'error'\n");
    script.append("console.error 'This is a test of log for ${a}'");
    db.command(new OCommandScript("sql", script.toString())).execute();
  }

  @Test
  public void testReturnObject() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("return [{ a: 'b' }]");
    Collection<Object> result = db.command(new OCommandScript("sql", script.toString())).execute();

    Assert.assertNotNull(result);

    Assert.assertEquals(result.size(), 1);

    Assert.assertTrue(result.iterator().next() instanceof Map);
  }

  @Test
  public void testIncrementAndLet() throws Exception {

    StringBuilder script = new StringBuilder();
    script.append("CREATE CLASS TestCounter;\n");
    script.append("INSERT INTO TestCounter set weight = 3;\n");
    script.append("LET counter = SELECT count(*) FROM TestCounter;\n");
    script.append("UPDATE TestCounter INCREMENT weight = $counter[0].count RETURN AfTER @this;\n");
    List<ODocument> qResult = db.command(new OCommandScript("sql", script.toString())).execute();

    assertThat(qResult.get(0).<Long>field("weight")).isEqualTo(4L);
  }

  @Test
  @Ignore
  public void testIncrementAndLetNewApi() throws Exception {

    StringBuilder script = new StringBuilder();
    script.append("CREATE CLASS TestCounter;\n");
    script.append("INSERT INTO TestCounter set weight = 3;\n");
    script.append("LET counter = SELECT count(*) FROM TestCounter;\n");
    script.append("UPDATE TestCounter INCREMENT weight = $counter[0].count RETURN AfTER @this;\n");
    OResultSet qResult = db.execute("sql", script.toString());

    assertThat(qResult.next().getElement().get().<Long>getProperty("weight")).isEqualTo(4L);
  }

  @Test
  public void testIf1() throws Exception {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 1 as one\n");
    script.append("if($a[0].one = 1){\n");
    script.append(" return 'OK'\n");
    script.append("}\n");
    script.append("return 'FAIL'\n");
    Object qResult = db.command(new OCommandScript("sql", script.toString())).execute();

    Assert.assertNotNull(qResult);
    Assert.assertEquals(qResult, "OK");
  }

  @Test
  public void testIf2() throws Exception {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 1 as one\n");
    script.append("if    ($a[0].one = 1)   { \n");
    script.append(" return 'OK'\n");
    script.append("     }      \n");
    script.append("return 'FAIL'\n");
    Object qResult = db.command(new OCommandScript("sql", script.toString())).execute();

    Assert.assertNotNull(qResult);
    Assert.assertEquals(qResult, "OK");
  }

  @Test
  public void testIf3() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("let $a = select 1 as one; if($a[0].one = 1){return 'OK';}return 'FAIL';");
    Object qResult = db.command(new OCommandScript("sql", script.toString())).execute();
    Assert.assertNotNull(qResult);
    Assert.assertEquals(qResult, "OK");
  }

  @Test
  public void testNestedIf2() throws Exception {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 1 as one\n");
    script.append("if($a[0].one = 1){\n");
    script.append("    if($a[0].one = 'zz'){\n");
    script.append("      return 'FAIL'\n");
    script.append("    }\n");
    script.append("  return 'OK'\n");
    script.append("}\n");
    script.append("return 'FAIL'\n");
    Object qResult = db.command(new OCommandScript("sql", script.toString())).execute();

    Assert.assertNotNull(qResult);
    Assert.assertEquals(qResult, "OK");
  }

  @Test
  public void testNestedIf3() throws Exception {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 1 as one\n");
    script.append("if($a[0].one = 'zz'){\n");
    script.append("    if($a[0].one = 1){\n");
    script.append("      return 'FAIL'\n");
    script.append("    }\n");
    script.append("  return 'FAIL'\n");
    script.append("}\n");
    script.append("return 'OK'\n");
    Object qResult = db.command(new OCommandScript("sql", script.toString())).execute();

    Assert.assertNotNull(qResult);
    Assert.assertEquals(qResult, "OK");
  }

  @Test
  public void testIfRealQuery() throws Exception {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select from foo\n");
    script.append("if($a is not null and $a.size() = 3){\n");
    script.append("  return $a\n");
    script.append("}\n");
    script.append("return 'FAIL'\n");
    Object qResult = db.command(new OCommandScript("sql", script.toString())).execute();

    Assert.assertNotNull(qResult);
    Assert.assertEquals(((List) qResult).size(), 3);
  }

  @Test
  public void testIfMultipleStatements() throws Exception {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 1 as one\n");
    script.append("if($a[0].one = 1){\n");
    script.append("  let $b = select 'OK' as ok\n");
    script.append("  return $b[0].ok\n");
    script.append("}\n");
    script.append("return 'FAIL'\n");
    Object qResult = db.command(new OCommandScript("sql", script.toString())).execute();

    Assert.assertNotNull(qResult);
    Assert.assertEquals(qResult, "OK");
  }

  @Test
  public void testScriptSubContext() throws Exception {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select from foo limit 1\n");
    script.append("select from (traverse doesnotexist from $a)\n");
    Iterable qResult = db.command(new OCommandScript("sql", script.toString())).execute();

    Assert.assertNotNull(qResult);
    Iterator iterator = qResult.iterator();
    Assert.assertTrue(iterator.hasNext());
    iterator.next();
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testSemicolonInString() throws Exception {
    // issue https://github.com/orientechnologies/orientjs/issues/133
    // testing parsing problem
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 'foo ; bar' as one\n");
    script.append("let $b = select 'foo \\\'; bar' as one\n");

    script.append("let $a = select \"foo ; bar\" as one\n");
    script.append("let $b = select \"foo \\\"; bar\" as one\n");
    Object qResult = db.command(new OCommandScript("sql", script.toString())).execute();
  }

  @Test
  public void testQuotedRegex() {
    // issue #4996 (simplified)
    db.command(new OCommandSQL("CREATE CLASS QuotedRegex2")).execute();
    String batch = "INSERT INTO QuotedRegex2 SET regexp=\"'';\"";

    db.command(new OCommandScript(batch.toString())).execute();

    List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("SELECT FROM QuotedRegex2"));
    Assert.assertEquals(result.size(), 1);
    ODocument doc = result.get(0);
    Assert.assertEquals(doc.field("regexp"), "'';");
  }

  @Test
  public void testParameters1() {
    String className = "testParameters1";
    db.createVertexClass(className);
    String script =
        "BEGIN;"
            + "LET $a = CREATE VERTEX "
            + className
            + " SET name = :name;"
            + "LET $b = CREATE VERTEX "
            + className
            + " SET name = :_name2;"
            + "LET $edge = CREATE EDGE E from $a to $b;"
            + "COMMIT;"
            + "RETURN $edge;";

    HashMap<String, Object> map = new HashMap<>();
    map.put("name", "bozo");
    map.put("_name2", "bozi");

    OResultSet rs = db.execute("sql", script, map);
    rs.close();

    rs = db.query("SELECT FROM " + className + " WHERE name = ?", "bozo");

    Assert.assertTrue(rs.hasNext());
    rs.next();
    rs.close();
  }

  @Test
  public void testPositionalParameters() {
    String className = "testPositionalParameters";
    db.createVertexClass(className);
    String script =
        "BEGIN;"
            + "LET $a = CREATE VERTEX "
            + className
            + " SET name = ?;"
            + "LET $b = CREATE VERTEX "
            + className
            + " SET name = ?;"
            + "LET $edge = CREATE EDGE E from $a to $b;"
            + "COMMIT;"
            + "RETURN $edge;";

    OResultSet rs = db.execute("sql", script, "bozo", "bozi");
    rs.close();

    rs = db.query("SELECT FROM " + className + " WHERE name = ?", "bozo");

    Assert.assertTrue(rs.hasNext());
    rs.next();
    rs.close();
  }
}
