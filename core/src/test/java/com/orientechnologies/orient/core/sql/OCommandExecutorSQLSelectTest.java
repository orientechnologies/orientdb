package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test
public class OCommandExecutorSQLSelectTest {
  private static String DB_STORAGE = "memory";
  private static String DB_NAME    = "OCommandExecutorSQLSelectTest";

  ODatabaseDocumentTx db;

  @BeforeClass
  public void beforeClass() throws Exception {
    db = new ODatabaseDocumentTx(DB_STORAGE + ":" + DB_NAME);
    db.create();
    getProfilerInstance().startRecording();

    db.command(new OCommandSQL("CREATE class foo")).execute();
    db.command(new OCommandSQL("CREATE property foo.name STRING")).execute();
    db.command(new OCommandSQL("CREATE property foo.bar INTEGER")).execute();
    db.command(new OCommandSQL("CREATE property foo.comp STRING")).execute();
    db.command(new OCommandSQL("CREATE property foo.osite INTEGER")).execute();

    db.command(new OCommandSQL("CREATE index foo_name on foo (name) NOTUNIQUE")).execute();
    db.command(new OCommandSQL("CREATE index foo_bar on foo (bar) NOTUNIQUE")).execute();
    db.command(new OCommandSQL("CREATE index foo_comp_osite on foo (comp, osite) NOTUNIQUE")).execute();

    db.command(new OCommandSQL("insert into foo (name, bar) values ('a', 1)")).execute();
    db.command(new OCommandSQL("insert into foo (name, bar) values ('b', 2)")).execute();
    db.command(new OCommandSQL("insert into foo (name, bar) values ('c', 3)")).execute();

    db.command(new OCommandSQL("insert into foo (comp, osite) values ('a', 1)")).execute();
    db.command(new OCommandSQL("insert into foo (comp, osite) values ('b', 2)")).execute();
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
  public void testUseIndexWithOr() throws Exception {
    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult = db.command(
        new OCommandSQL("select * from foo where bar = 2 or name ='a' and bar >= 0")).execute();

    assertEquals(qResult.size(), 2);
    assertEquals(indexUsages(db), idxUsagesBefore + 2);
  }

  @Test
  public void testDoNotUseIndexWithOrNotIndexed() throws Exception {

    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult = db.command(
        new OCommandSQL("select * from foo where bar = 2 or notIndexed = 3")).execute();

    assertEquals(indexUsages(db), idxUsagesBefore);
  }

  @Test
  public void testCompositeIndex() {
    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult = db.command(
        new OCommandSQL("select * from foo where comp = 'a' and osite = 1")).execute();

    assertEquals(qResult.size(), 1);
    assertEquals(indexUsages(db), idxUsagesBefore + 1);
  }


  @Test
  public void testProjection() {
    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult = db.command(
        new OCommandSQL("select a from foo where name = 'a' or bar = 1")).execute();

    assertEquals(qResult.size(), 1);
    assertEquals(indexUsages(db), idxUsagesBefore + 2);
  }

  @Test
  public void testProjection2() {
    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult = db.command(
        new OCommandSQL("select a from foo where name = 'a' or bar = 2")).execute();

    assertEquals(qResult.size(), 2);
    assertEquals(indexUsages(db), idxUsagesBefore + 2);
  }



  @Test
  public void testCompositeIndex2() {
    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult = db.command(
        new OCommandSQL("select * from foo where (comp = 'a' and osite = 1) or name = 'a'")).execute();

    assertEquals(qResult.size(), 2);
    assertEquals(indexUsages(db), idxUsagesBefore + 2);
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

  private OProfilerMBean getProfilerInstance() throws Exception {
    return Orient.instance().getProfiler();

  }
}
