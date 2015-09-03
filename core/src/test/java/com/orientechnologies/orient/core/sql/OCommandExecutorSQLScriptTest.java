package com.orientechnologies.orient.core.sql;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;

@Test
public class OCommandExecutorSQLScriptTest {
  private static String DB_STORAGE = "memory";
  private static String DB_NAME    = "OCommandExecutorSQLScriptTest";

  ODatabaseDocumentTx   db;

  @BeforeClass
  public void beforeClass() throws Exception {
    db = new ODatabaseDocumentTx(DB_STORAGE + ":" + DB_NAME);
    db.create();

    db.command(new OCommandSQL("CREATE class foo")).execute();

    db.command(new OCommandSQL("insert into foo (name, bar) values ('a', 1)")).execute();
    db.command(new OCommandSQL("insert into foo (name, bar) values ('b', 2)")).execute();
    db.command(new OCommandSQL("insert into foo (name, bar) values ('c', 3)")).execute();

  }

  @AfterClass
  public void afterClass() throws Exception {
    if (db.isClosed()) {
      db.open("admin", "admin");
    }
    db.command(new OCommandSQL("drop class foo")).execute();
    db.getMetadata().getSchema().reload();
    db.close();
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
    List<String> result = db.command(new OCommandScript("sql", script.toString())).execute();

    Assert.assertNotNull(result);
    for (String d : result) {
      new ODocument().fromJSON(d);
    }
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
    script.append("console.log This is a test of log for ${a}");
    db.command(new OCommandScript("sql", script.toString())).execute();
  }

  @Test
  public void testConsoleOutput() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("LET $a = 'output'\n");
    script.append("console.output This is a test of log for ${a}");
    db.command(new OCommandScript("sql", script.toString())).execute();
  }

  @Test
  public void testConsoleError() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("LET $a = 'error'\n");
    script.append("console.error This is a test of log for ${a}");
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

    Assert.assertEquals(qResult.get(0).field("weight"), 4l);
  }
}
