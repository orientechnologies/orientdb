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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test
public class OCommandExecutorSQLSelectTest {
  private static String DB_STORAGE = "memory";
  private static String DB_NAME    = "OCommandExecutorSQLSelectTest";

  ODatabaseDocumentTx   db;

  @BeforeClass
  public void beforeClass() throws Exception {
    db = new ODatabaseDocumentTx(DB_STORAGE + ":" + DB_NAME);
    db.create();
    getProfilerInstance().startRecording();

    db.command(new OCommandSQL("CREATE class foo")).execute();
    db.command(new OCommandSQL("CREATE property foo.name STRING")).execute();
    db.command(new OCommandSQL("CREATE property foo.bar INTEGER")).execute();
    db.command(new OCommandSQL("CREATE property foo.address EMBEDDED")).execute();
    db.command(new OCommandSQL("CREATE property foo.comp STRING")).execute();
    db.command(new OCommandSQL("CREATE property foo.osite INTEGER")).execute();

    db.command(new OCommandSQL("CREATE index foo_name on foo (name) NOTUNIQUE")).execute();
    db.command(new OCommandSQL("CREATE index foo_bar on foo (bar) NOTUNIQUE")).execute();
    db.command(new OCommandSQL("CREATE index foo_comp_osite on foo (comp, osite) NOTUNIQUE")).execute();

    db.command(
        new OCommandSQL("insert into foo (name, bar, address) values ('a', 1, {'street':'1st street', 'city':'NY', '@type':'d'})"))
        .execute();
    db.command(new OCommandSQL("insert into foo (name, bar) values ('b', 2)")).execute();
    db.command(new OCommandSQL("insert into foo (name, bar) values ('c', 3)")).execute();

    db.command(new OCommandSQL("insert into foo (comp, osite) values ('a', 1)")).execute();
    db.command(new OCommandSQL("insert into foo (comp, osite) values ('b', 2)")).execute();

    db.command(new OCommandSQL("CREATE class bar")).execute();

    db.command(new OCommandSQL("insert into bar (name, foo) values ('a', 1)")).execute();
    db.command(new OCommandSQL("insert into bar (name, foo) values ('b', 2)")).execute();
    db.command(new OCommandSQL("insert into bar (name, foo) values ('c', 3)")).execute();
    db.command(new OCommandSQL("insert into bar (name, foo) values ('d', 4)")).execute();
    db.command(new OCommandSQL("insert into bar (name, foo) values ('e', 5)")).execute();
    db.command(new OCommandSQL("insert into bar (name, foo) values ('f', 1)")).execute();
    db.command(new OCommandSQL("insert into bar (name, foo) values ('g', 2)")).execute();
    db.command(new OCommandSQL("insert into bar (name, foo) values ('h', 3)")).execute();
    db.command(new OCommandSQL("insert into bar (name, foo) values ('i', 4)")).execute();
    db.command(new OCommandSQL("insert into bar (name, foo) values ('j', 5)")).execute();
    db.command(new OCommandSQL("insert into bar (name, foo) values ('k', 1)")).execute();
    db.command(new OCommandSQL("insert into bar (name, foo) values ('l', 2)")).execute();
    db.command(new OCommandSQL("insert into bar (name, foo) values ('m', 3)")).execute();
    db.command(new OCommandSQL("insert into bar (name, foo) values ('n', 4)")).execute();
    db.command(new OCommandSQL("insert into bar (name, foo) values ('o', 5)")).execute();

    db.command(new OCommandSQL("CREATE class ridsorttest")).execute();
    db.command(new OCommandSQL("CREATE property ridsorttest.name INTEGER")).execute();
    db.command(new OCommandSQL("CREATE index ridsorttest_name on ridsorttest (name) NOTUNIQUE")).execute();

    db.command(new OCommandSQL("insert into ridsorttest (name) values (1)")).execute();
    db.command(new OCommandSQL("insert into ridsorttest (name) values (5)")).execute();
    db.command(new OCommandSQL("insert into ridsorttest (name) values (3)")).execute();
    db.command(new OCommandSQL("insert into ridsorttest (name) values (4)")).execute();
    db.command(new OCommandSQL("insert into ridsorttest (name) values (1)")).execute();
    db.command(new OCommandSQL("insert into ridsorttest (name) values (8)")).execute();
    db.command(new OCommandSQL("insert into ridsorttest (name) values (6)")).execute();

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
  public void testUseIndexWithOrderBy2() throws Exception {
    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult = db.command(new OCommandSQL("select * from foo where address.city = 'NY' order by name ASC"))
        .execute();
    assertEquals(qResult.size(), 1);
  }

  @Test
  public void testUseIndexWithOr() throws Exception {
    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult = db.command(new OCommandSQL("select * from foo where bar = 2 or name ='a' and bar >= 0")).execute();

    assertEquals(qResult.size(), 2);
    assertEquals(indexUsages(db), idxUsagesBefore + 2);
  }

  @Test
  public void testDoNotUseIndexWithOrNotIndexed() throws Exception {

    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult = db.command(new OCommandSQL("select * from foo where bar = 2 or notIndexed = 3")).execute();

    assertEquals(indexUsages(db), idxUsagesBefore);
  }

  @Test
  public void testCompositeIndex() {
    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult = db.command(new OCommandSQL("select * from foo where comp = 'a' and osite = 1")).execute();

    assertEquals(qResult.size(), 1);
    assertEquals(indexUsages(db), idxUsagesBefore + 1);
  }

  @Test
  public void testProjection() {
    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult = db.command(new OCommandSQL("select a from foo where name = 'a' or bar = 1")).execute();

    assertEquals(qResult.size(), 1);
    assertEquals(indexUsages(db), idxUsagesBefore + 2);
  }

  @Test
  public void testProjection2() {
    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult = db.command(new OCommandSQL("select a from foo where name = 'a' or bar = 2")).execute();

    assertEquals(qResult.size(), 2);
    assertEquals(indexUsages(db), idxUsagesBefore + 2);
  }

  @Test
  public void testCompositeIndex2() {
    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult = db.command(new OCommandSQL("select * from foo where (comp = 'a' and osite = 1) or name = 'a'"))
        .execute();

    assertEquals(qResult.size(), 2);
    assertEquals(indexUsages(db), idxUsagesBefore + 2);
  }

  @Test
  public void testOperatorPriority() {

    List<ODocument> qResult = db.command(new OCommandSQL("select * from foo where name ='a' and bar = 1000 or name = 'b'"))
        .execute();

    List<ODocument> qResult2 = db.command(new OCommandSQL("select * from foo where name = 'b' or name ='a' and bar = 1000"))
        .execute();

    List<ODocument> qResult3 = db.command(new OCommandSQL("select * from foo where name = 'b' or (name ='a' and bar = 1000)"))
        .execute();

    List<ODocument> qResult4 = db.command(new OCommandSQL("select * from foo where (name ='a' and bar = 1000) or name = 'b'"))
        .execute();

    List<ODocument> qResult5 = db.command(new OCommandSQL("select * from foo where ((name ='a' and bar = 1000) or name = 'b')"))
        .execute();

    List<ODocument> qResult6 = db.command(new OCommandSQL("select * from foo where ((name ='a' and (bar = 1000)) or name = 'b')"))
        .execute();

    List<ODocument> qResult7 = db.command(new OCommandSQL("select * from foo where (((name ='a' and bar = 1000)) or name = 'b')"))
        .execute();

    List<ODocument> qResult8 = db
        .command(new OCommandSQL("select * from foo where (((name ='a' and bar = 1000)) or (name = 'b'))")).execute();

    assertEquals(qResult.size(), qResult2.size());
    assertEquals(qResult.size(), qResult3.size());
    assertEquals(qResult.size(), qResult4.size());
    assertEquals(qResult.size(), qResult5.size());
    assertEquals(qResult.size(), qResult6.size());
    assertEquals(qResult.size(), qResult7.size());
    assertEquals(qResult.size(), qResult8.size());

  }

  @Test
  public void testOperatorPriority2() {
    List<ODocument> qResult = db
        .command(new OCommandSQL("select * from bar where name ='a' and foo = 1 or name='b' or name='c' and foo = 3 and other = 4 or name = 'e' and foo = 5 or name = 'm' and foo > 2 "))
        .execute();

    List<ODocument> qResult2 = db
        .command(
            new OCommandSQL(
                "select * from bar where (name ='a' and foo = 1) or name='b' or (name='c' and foo = 3 and other = 4) or (name = 'e' and foo = 5) or (name = 'm' and foo > 2)"))
        .execute();

    List<ODocument> qResult3 = db
        .command(
            new OCommandSQL(
                "select * from bar where (name ='a' and foo = 1) or (name='b') or (name='c' and foo = 3 and other = 4) or (name ='e' and foo = 5) or (name = 'm' and foo > 2)"))
        .execute();

    List<ODocument> qResult4 = db
        .command(
            new OCommandSQL(
                "select * from bar where (name ='a' and foo = 1) or ((name='b') or (name='c' and foo = 3 and other = 4)) or (name = 'e' and foo = 5) or (name = 'm' and foo > 2)"))
        .execute();

    List<ODocument> qResult5 = db
        .command(
            new OCommandSQL(
                "select * from bar where (name ='a' and foo = 1) or ((name='b') or (name='c' and foo = 3 and other = 4) or (name = 'e' and foo = 5)) or (name = 'm' and foo > 2)"))
        .execute();

    assertEquals(qResult.size(), qResult2.size());
    assertEquals(qResult.size(), qResult3.size());
    assertEquals(qResult.size(), qResult4.size());
    assertEquals(qResult.size(), qResult5.size());

  }

  @Test
  public void testOperatorPriority3() {
    List<ODocument> qResult = db
        .command(new OCommandSQL("select * from bar where name <> 'a' and foo = 1 or name='b' or name='c' and foo = 3 and other = 4 or name = 'e' and foo = 5 or name = 'm' and foo > 2 "))
        .execute();

    List<ODocument> qResult2 = db
        .command(
            new OCommandSQL(
                "select * from bar where (name <> 'a' and foo = 1) or name='b' or (name='c' and foo = 3 and other <>  4) or (name = 'e' and foo = 5) or (name = 'm' and foo > 2)"))
        .execute();

    List<ODocument> qResult3 = db
        .command(
            new OCommandSQL(
                "select * from bar where ( name <> 'a' and foo = 1) or (name='b') or (name='c' and foo = 3 and other <>  4) or (name ='e' and foo = 5) or (name = 'm' and foo > 2)"))
        .execute();

    List<ODocument> qResult4 = db
        .command(
            new OCommandSQL(
                "select * from bar where (name <> 'a' and foo = 1) or ( (name='b') or (name='c' and foo = 3 and other <>  4)) or  (name = 'e' and foo = 5) or (name = 'm' and foo > 2)"))
        .execute();

    List<ODocument> qResult5 = db
        .command(
            new OCommandSQL(
                "select * from bar where (name <> 'a' and foo = 1) or ((name='b') or (name='c' and foo = 3 and other <>  4) or (name = 'e' and foo = 5)) or (name = 'm' and foo > 2)"))
        .execute();

    assertEquals(qResult.size(), qResult2.size());
    assertEquals(qResult.size(), qResult3.size());
    assertEquals(qResult.size(), qResult4.size());
    assertEquals(qResult.size(), qResult5.size());

  }

  @Test
  public void testExpandOnEmbedded() {
    List<ODocument> qResult = db.command(new OCommandSQL("select expand(address) from foo where name = 'a'")).execute();

    assertEquals(qResult.size(), 1);
    assertEquals(qResult.get(0).field("city"), "NY");
  }

  @Test
  public void testFlattenOnEmbedded() {
    List<ODocument> qResult = db.command(new OCommandSQL("select flatten(address) from foo where name = 'a'")).execute();

    assertEquals(qResult.size(), 1);
    assertEquals(qResult.get(0).field("city"), "NY");
  }

  @Test
  public void testLimit() {
    List<ODocument> qResult = db.command(new OCommandSQL("select from foo limit 3")).execute();
    assertEquals(qResult.size(), 3);
  }

  @Test
  public void testLimitWithMetadataQuery() {
    List<ODocument> qResult = db.command(new OCommandSQL("select expand(classes) from metadata:schema limit 3")).execute();
    assertEquals(qResult.size(), 3);
  }

  @Test
  public void testOrderByRid() {
    List<ODocument> qResult = db.command(new OCommandSQL("select from ridsorttest order by @rid ASC")).execute();
    assertTrue(qResult.size() > 0);

    ODocument prev = qResult.get(0);
    for (int i = 1; i < qResult.size(); i++) {
      assertTrue(prev.getIdentity().compareTo(qResult.get(i).getIdentity()) <= 0);
      prev = qResult.get(i);
    }

    qResult = db.command(new OCommandSQL("select from ridsorttest order by @rid DESC")).execute();
    assertTrue(qResult.size() > 0);

    prev = qResult.get(0);
    for (int i = 1; i < qResult.size(); i++) {
      assertTrue(prev.getIdentity().compareTo(qResult.get(i).getIdentity()) >= 0);
      prev = qResult.get(i);
    }

    qResult = db.command(new OCommandSQL("select from ridsorttest where name > 3 order by @rid DESC")).execute();
    assertTrue(qResult.size() > 0);

    prev = qResult.get(0);
    for (int i = 1; i < qResult.size(); i++) {
      assertTrue(prev.getIdentity().compareTo(qResult.get(i).getIdentity()) >= 0);
      prev = qResult.get(i);
    }
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
