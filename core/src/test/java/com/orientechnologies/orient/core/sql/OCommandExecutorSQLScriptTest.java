package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
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

  ODatabaseDocumentTx   db;

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
}
