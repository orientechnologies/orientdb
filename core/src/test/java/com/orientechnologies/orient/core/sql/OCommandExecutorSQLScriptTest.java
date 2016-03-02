package com.orientechnologies.orient.core.sql;


import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test
public class OCommandExecutorSQLScriptTest {
  private static String DB_STORAGE = "memory";
  private static String DB_NAME    = "OCommandExecutorSQLScriptTest";

  ODatabaseDocumentTx db;

  @BeforeClass
  public void beforeClass() throws Exception {
    db = new ODatabaseDocumentTx(DB_STORAGE + ":" + DB_NAME);
    db.create();
    getProfilerInstance().startRecording();

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

    assertEquals(qResult.size(), 3);
  }

  @Test
  public void testIncrementAndLet() throws Exception {

    StringBuilder script = new StringBuilder();
    script.append("CREATE CLASS TestCounter;\n");
    script.append("INSERT INTO TestCounter set weight = 3;\n");
    script.append("LET counter = SELECT count(*) FROM TestCounter;\n");
    script.append("UPDATE TestCounter INCREMENT weight = $counter[0].count RETURN AfTER @this;\n");
    List<ODocument> qResult = db.command(new OCommandScript("sql", script.toString())).execute();

    assertEquals(qResult.get(0).field("weight"), 4l);
  }

  private long indexUsages(ODatabaseDocumentTx db) {
    final long oldIndexUsage;
    try {
      oldIndexUsage = getProfilerInstance().getCounter("db." + DB_NAME + ".query.indexUsed");
      return oldIndexUsage == -1 ? 0 : oldIndexUsage;
    } catch (Exception e) {
      fail();
    }
    return -1l;
  }

  private OProfiler getProfilerInstance() throws Exception {
    return Orient.instance().getProfiler();

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
    Assert.assertEquals(((List)qResult).size(), 3);
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
  public void testSemicolonInString() throws Exception {
    //issue https://github.com/orientechnologies/orientjs/issues/133
    //testing parsing problem
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 'foo ; bar' as one\n");
    script.append("let $b = select 'foo \\\'; bar' as one\n");

    script.append("let $a = select \"foo ; bar\" as one\n");
    script.append("let $b = select \"foo \\\"; bar\" as one\n");
    Object qResult = db.command(new OCommandScript("sql", script.toString())).execute();
  }

  @Test
  public void testQuotedRegex() {
    //issue #4996 (simplified)
    db.command(new OCommandSQL("CREATE CLASS QuotedRegex2")).execute();
    String batch = "INSERT INTO QuotedRegex2 SET regexp=\"'';\"";

    db.command(new OCommandScript(batch.toString())).execute();

    List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("SELECT FROM QuotedRegex2"));
    Assert.assertEquals(result.size(), 1);
    ODocument doc = result.get(0);
    Assert.assertEquals(doc.field("regexp"), "'';");
  }

}
