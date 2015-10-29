package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.testng.Assert.*;

@Test
public class OCommandExecutorSQLSelectTest {
  private static String DB_STORAGE             = "memory";
  private static String DB_NAME                = "OCommandExecutorSQLSelectTest";

  private int           ORDER_SKIP_LIMIT_ITEMS = 100 * 1000;

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

    db.command(new OCommandSQL("CREATE class unwindtest")).execute();
    db.command(new OCommandSQL("insert into unwindtest (name, coll) values ('foo', ['foo1', 'foo2'])")).execute();
    db.command(new OCommandSQL("insert into unwindtest (name, coll) values ('bar', ['bar1', 'bar2'])")).execute();

    db.command(new OCommandSQL("CREATE class edge")).execute();

    db.command(new OCommandSQL("CREATE class TestFromInSquare")).execute();
    db.command(new OCommandSQL("insert into TestFromInSquare set tags = {' from ':'foo',' to ':'bar'}")).execute();

    db.command(new OCommandSQL("CREATE class TestMultipleClusters")).execute();
    db.command(new OCommandSQL("alter class TestMultipleClusters addcluster testmultipleclusters1 ")).execute();
    db.command(new OCommandSQL("alter class TestMultipleClusters addcluster testmultipleclusters2 ")).execute();
    db.command(new OCommandSQL("insert into cluster:testmultipleclusters set name = 'aaa'")).execute();
    db.command(new OCommandSQL("insert into cluster:testmultipleclusters1 set name = 'foo'")).execute();
    db.command(new OCommandSQL("insert into cluster:testmultipleclusters2 set name = 'bar'")).execute();

    db.command(new OCommandSQL("CREATE class TestUrl")).execute();
    db.command(new OCommandSQL("insert into TestUrl content { \"url\": \"http://www.google.com\" }")).execute();

    db.command(new OCommandSQL("CREATE class TestParams")).execute();
    db.command(new OCommandSQL("insert into TestParams  set name = 'foo', surname ='foo', active = true")).execute();
    db.command(new OCommandSQL("insert into TestParams  set name = 'foo', surname ='bar', active = false")).execute();

    db.command(new OCommandSQL("CREATE class TestParamsEmbedded")).execute();
    db.command(
        new OCommandSQL("insert into TestParamsEmbedded set emb = {  \n" + "            \"count\":0,\n"
            + "            \"testupdate\":\"1441258203385\"\n" + "         }")).execute();
    db.command(
        new OCommandSQL("insert into TestParamsEmbedded set emb = {  \n" + "            \"count\":1,\n"
            + "            \"testupdate\":\"1441258203385\"\n" + "         }")).execute();

    db.command(new OCommandSQL("CREATE class TestBacktick")).execute();
    db.command(new OCommandSQL("insert into TestBacktick  set foo = 1, bar = 2, `foo-bar` = 10")).execute();

    // /*** from issue #2743
    OSchema schema = db.getMetadata().getSchema();
    if (!schema.existsClass("alphabet")) {
      schema.createClass("alphabet");
    }

    ORecordIteratorClass<ODocument> iter = db.browseClass("alphabet");
    while (iter.hasNext()) {
      iter.next().delete();
    }

    // add 26 entries: { "letter": "A", "number": 0 }, ... { "letter": "Z", "number": 25 }

    String rowModel = "{\"letter\": \"%s\", \"number\": %d}";
    for (int i = 0; i < 26; ++i) {
      String l = String.valueOf((char) ('A' + i));
      String json = String.format(rowModel, l, i);
      ODocument doc = db.newInstance("alphabet");
      doc.fromJSON(json);
      doc.save();
    }

    db.command(new OCommandSQL("create class OCommandExecutorSQLSelectTest_aggregations")).execute();
    db.command(
        new OCommandSQL(
            "insert into OCommandExecutorSQLSelectTest_aggregations set data = [{\"size\": 0}, {\"size\": 0}, {\"size\": 30}, {\"size\": 50}, {\"size\": 50}]"))
        .execute();

    initExpandSkipLimit(db);

    initMassiveOrderSkipLimit(db);
    initDatesSet(db);

    initMatchesWithRegex(db);
  }

  private void initMatchesWithRegex(ODatabaseInternal<ORecord> db) {
    db.command(new OCommandSQL("CREATE class matchesstuff")).execute();

    db.command(new OCommandSQL("insert into matchesstuff (name, foo) values ('admin[name]', 1)")).execute();
  }

  private void initDatesSet(ODatabaseDocumentTx db) {
    db.command(new OCommandSQL("create class OCommandExecutorSQLSelectTest_datesSet")).execute();
    db.command(new OCommandSQL("create property OCommandExecutorSQLSelectTest_datesSet.foo embeddedlist date")).execute();
    db.command(new OCommandSQL("insert into OCommandExecutorSQLSelectTest_datesSet set foo = ['2015-10-21']")).execute();
  }

  private void initMassiveOrderSkipLimit(ODatabaseDocumentTx db) {
    db.getMetadata().getSchema().createClass("MassiveOrderSkipLimit");
    db.declareIntent(new OIntentMassiveInsert());
    String fieldValue = "laskdf lkajsd flaksjdf laksjd flakjsd flkasjd flkajsd flkajsd flkajsd flkajsd flkajsd flkjas;lkj a;ldskjf laksdj asdklasdjf lskdaj fladsd";
    for (int i = 0; i < ORDER_SKIP_LIMIT_ITEMS; i++) {
      ODocument doc = new ODocument("MassiveOrderSkipLimit");
      doc.field("nnum", i);
      doc.field("aaa", fieldValue);
      doc.field("bbb", fieldValue);
      doc.field("bbba", fieldValue);
      doc.field("daf", fieldValue);
      doc.field("dfgd", fieldValue);
      doc.field("dgd", fieldValue);

      doc.save();
    }
    db.declareIntent(null);
  }

  private void initExpandSkipLimit(ODatabaseDocumentTx db) {
    db.getMetadata().getSchema().createClass("ExpandSkipLimit");

    for (int i = 0; i < 5; i++) {
      ODocument doc = new ODocument("ExpandSkipLimit");
      doc.field("nnum", i);
      doc.save();
      ODocument parent = new ODocument("ExpandSkipLimit");
      parent.field("parent", true);
      parent.field("num", i);
      parent.field("linked", doc);
      parent.save();
    }
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
        .command(
            new OCommandSQL(
                "select * from bar where name ='a' and foo = 1 or name='b' or name='c' and foo = 3 and other = 4 or name = 'e' and foo = 5 or name = 'm' and foo > 2 "))
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
        .command(
            new OCommandSQL(
                "select * from bar where name <> 'a' and foo = 1 or name='b' or name='c' and foo = 3 and other = 4 or name = 'e' and foo = 5 or name = 'm' and foo > 2 "))
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
  public void testOrderByWithMetadataQuery() {
    List<ODocument> qResult = db.command(new OCommandSQL("select expand(classes) from metadata:schema order by name")).execute();
    assertTrue(qResult.size() > 0);
  }

  @Test
  public void testLimitWithUnnamedParam() {
    List<ODocument> qResult = db.command(new OCommandSQL("select from foo limit ?")).execute(3);
    assertEquals(qResult.size(), 3);
  }

  @Test
  public void testLimitWithNamedParam() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("lim", 2);
    List<ODocument> qResult = db.command(new OCommandSQL("select from foo limit :lim")).execute(params);
    assertEquals(qResult.size(), 2);
  }

  @Test
  public void testParamsInLetSubquery() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "foo");
    List<ODocument> qResult = db.command(
        new OCommandSQL(
            "select from TestParams let $foo = (select name from TestParams where surname = :name) where surname in $foo.name "))
        .execute(params);
    assertEquals(qResult.size(), 1);
  }

  @Test
  public void testBooleanParams() {
    // issue #4224
    List<ODocument> qResult = db.command(new OCommandSQL("select name from TestParams where name = ? and active = ?")).execute(
        "foo", true);
    assertEquals(qResult.size(), 1);
  }

  @Test
  public void testOrderByEmbeddedParams() {
    // issue #4949
    Map<String, Object> parameters = new HashMap<String, Object>();
    parameters.put("paramvalue", "count");
    List<ODocument> qResult = db.command(new OCommandSQL("select from TestParamsEmbedded order by emb[:paramvalue] DESC")).execute(
        parameters);
    assertEquals(qResult.size(), 2);
    Map embedded = qResult.get(0).field("emb");
    assertEquals(embedded.get("count"), 1);
  }

  @Test
  public void testOrderByEmbeddedParams2() {
    // issue #4949
    Map<String, Object> parameters = new HashMap<String, Object>();
    parameters.put("paramvalue", "count");
    List<ODocument> qResult = db.command(new OCommandSQL("select from TestParamsEmbedded order by emb[:paramvalue] ASC")).execute(
        parameters);
    assertEquals(qResult.size(), 2);
    Map embedded = qResult.get(0).field("emb");
    assertEquals(embedded.get("count"), 0);
  }

  @Test
  public void testFromInSquareBrackets() {
    List<ODocument> qResult = db.command(new OCommandSQL("select tags[' from '] as a from TestFromInSquare")).execute();
    assertEquals(qResult.size(), 1);
    assertEquals(qResult.get(0).field("a"), "foo");
  }

  @Test
  public void testNewline() {
    List<ODocument> qResult = db.command(new OCommandSQL("select\n1 as ACTIVE\nFROM foo")).execute();
    assertEquals(qResult.size(), 5);
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

  @Test
  public void testUnwind() {
    List<ODocument> qResult = db.command(new OCommandSQL("select from unwindtest unwind coll")).execute();

    assertEquals(qResult.size(), 4);
    for (ODocument doc : qResult) {
      String name = doc.field("name");
      String coll = doc.field("coll");
      assertTrue(coll.startsWith(name));
      assertFalse(doc.getIdentity().isPersistent());
    }
  }

  @Test
  public void testUnwindOrder() {
    List<ODocument> qResult = db.command(new OCommandSQL("select from unwindtest order by coll unwind coll")).execute();

    assertEquals(qResult.size(), 4);
    for (ODocument doc : qResult) {
      String name = doc.field("name");
      String coll = doc.field("coll");
      assertTrue(coll.startsWith(name));
      assertFalse(doc.getIdentity().isPersistent());
    }
  }

  @Test
  public void testUnwindSkip() {
    List<ODocument> qResult = db.command(new OCommandSQL("select from unwindtest unwind coll skip 1")).execute();

    assertEquals(qResult.size(), 3);
    for (ODocument doc : qResult) {
      String name = doc.field("name");
      String coll = doc.field("coll");
      assertTrue(coll.startsWith(name));
    }
  }

  @Test
  public void testUnwindLimit() {
    List<ODocument> qResult = db.command(new OCommandSQL("select from unwindtest unwind coll limit 1")).execute();

    assertEquals(qResult.size(), 1);
    for (ODocument doc : qResult) {
      String name = doc.field("name");
      String coll = doc.field("coll");
      assertTrue(coll.startsWith(name));
    }
  }

  @Test
  public void testUnwindLimit3() {
    List<ODocument> qResult = db.command(new OCommandSQL("select from unwindtest unwind coll limit 3")).execute();

    assertEquals(qResult.size(), 3);
    for (ODocument doc : qResult) {
      String name = doc.field("name");
      String coll = doc.field("coll");
      assertTrue(coll.startsWith(name));
    }
  }

  @Test
  public void testUnwindSkipAndLimit() {
    List<ODocument> qResult = db.command(new OCommandSQL("select from unwindtest unwind coll skip 1 limit 1")).execute();

    assertEquals(qResult.size(), 1);
    for (ODocument doc : qResult) {
      String name = doc.field("name");
      String coll = doc.field("coll");
      assertTrue(coll.startsWith(name));
    }
  }

  @Test
  public void testMultipleClusters() {
    List<ODocument> qResult = db.command(new OCommandSQL("select from cluster:[testmultipleclusters1]")).execute();

    assertEquals(qResult.size(), 1);

    qResult = db.command(new OCommandSQL("select from cluster:[testmultipleclusters1, testmultipleclusters2]")).execute();

    assertEquals(qResult.size(), 2);

  }

  @Test
  public void testMatches() {
    List<?> result = db.query(new OSQLSynchQuery<Object>(
        "select from foo where name matches '(?i)(^\\\\Qa\\\\E$)|(^\\\\Qname2\\\\E$)|(^\\\\Qname3\\\\E$)' and bar = 1"));
    assertEquals(result.size(), 1);
  }

  @Test
  public void testStarPosition() {
    List<ODocument> result = db.query(new OSQLSynchQuery<Object>("select *, name as blabla from foo where name = 'a'"));

    assertEquals(result.size(), 1);
    assertEquals(result.get(0).field("blabla"), "a");

    result = db.query(new OSQLSynchQuery<Object>("select name as blabla, * from foo where name = 'a'"));

    assertEquals(result.size(), 1);
    assertEquals(result.get(0).field("blabla"), "a");

    result = db.query(new OSQLSynchQuery<Object>("select name as blabla, *, fff as zzz from foo where name = 'a'"));

    assertEquals(result.size(), 1);
    assertEquals(result.get(0).field("blabla"), "a");

  }

  @Test
  public void testQuotedClassName() {
    List<ODocument> qResult = db.command(new OCommandSQL("select from `edge`")).execute();

    assertEquals(qResult.size(), 0);

  }

  public void testUrl() {

    List<ODocument> qResult = db.command(new OCommandSQL("select from TestUrl")).execute();

    assertEquals(qResult.size(), 1);
    assertEquals(qResult.get(0).field("url"), "http://www.google.com");

  }

  @Test
  public void testUnwindSkipAndLimit2() {
    List<ODocument> qResult = db.command(new OCommandSQL("select from unwindtest unwind coll skip 1 limit 2")).execute();

    assertEquals(qResult.size(), 2);
    for (ODocument doc : qResult) {
      String name = doc.field("name");
      String coll = doc.field("coll");
      assertTrue(coll.startsWith(name));
    }
  }

  public void testMultipleParamsWithSameName() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("param1", "foo");
    List<ODocument> qResult = db.command(new OCommandSQL("select from TestParams where name like '%' + :param1 + '%'")).execute(
        params);
    assertEquals(qResult.size(), 2);

    qResult = db.command(
        new OCommandSQL("select from TestParams where name like '%' + :param1 + '%' and surname like '%' + :param1 + '%'"))
        .execute(params);
    assertEquals(qResult.size(), 1);

    params = new HashMap<String, Object>();
    params.put("param1", "bar");

    qResult = db.command(new OCommandSQL("select from TestParams where surname like '%' + :param1 + '%'")).execute(params);
    assertEquals(qResult.size(), 1);
  }

  // /*** from issue #2743
  @Test
  public void testBasicQueryOrdered() {
    OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet ORDER BY letter");
    List<ODocument> results = db.query(sql);
    assertEquals(26, results.size());
  }

  @Test
  public void testSkipZeroOrdered() {
    OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet ORDER BY letter SKIP 0");
    List<ODocument> results = db.query(sql);
    assertEquals(26, results.size());
  }

  @Test
  public void testSkipOrdered() {
    OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet ORDER BY letter SKIP 7");
    List<ODocument> results = db.query(sql);
    assertEquals(19, results.size()); // FAILURE - actual 0
  }

  @Test
  public void testLimitOrdered() {
    OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet ORDER BY letter LIMIT 9");
    List<ODocument> results = db.query(sql);
    assertEquals(9, results.size());
  }

  @Test
  public void testLimitMinusOneOrdered() {
    OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet ORDER BY letter LIMIT -1");
    List<ODocument> results = db.query(sql);
    assertEquals(26, results.size());
  }

  @Test
  public void testSkipAndLimitOrdered() {
    OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet ORDER BY letter SKIP 7 LIMIT 9");
    List<ODocument> results = db.query(sql);
    assertEquals(9, results.size());
  }

  @Test
  public void testSkipAndLimitMinusOneOrdered() {
    OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet ORDER BY letter SKIP 7 LIMIT -1");
    List<ODocument> results = db.query(sql);
    assertEquals(19, results.size());
  }

  @Test
  public void testLetAsListAsString() {
    OSQLSynchQuery sql = new OSQLSynchQuery(
        "SELECT $ll as lll from unwindtest let $ll = coll.asList().asString() where name = 'bar'");
    List<ODocument> results = db.query(sql);
    assertEquals(1, results.size());
    assertNotNull(results.get(0).field("lll"));
    assertEquals("[bar1, bar2]", results.get(0).field("lll"));
  }

  @Test
  public void testAggregations() {
    OSQLSynchQuery sql = new OSQLSynchQuery(
        "select data.size as collection_content, data.size() as collection_size, min(data.size) as collection_min, max(data.size) as collection_max, sum(data.size) as collection_sum, avg(data.size) as collection_avg from OCommandExecutorSQLSelectTest_aggregations");
    List<ODocument> results = db.query(sql);
    assertEquals(1, results.size());
    ODocument doc = results.get(0);
    assertEquals(5, doc.field("collection_size"));
    assertEquals(130, doc.field("collection_sum"));
    assertEquals(26, doc.field("collection_avg"));
    assertEquals(0, doc.field("collection_min"));
    assertEquals(50, doc.field("collection_max"));
  }

  @Test
  public void testLetOrder() {
    OSQLSynchQuery sql = new OSQLSynchQuery("SELECT" + "      source," + "  $maxYear as maxYear" + "              FROM" + "      ("
        + "          SELECT expand( $union ) " + "  LET" + "      $a = (SELECT 'A' as source, 2013 as year),"
        + "  $b = (SELECT 'B' as source, 2012 as year)," + "  $union = unionAll($a,$b) " + "  ) " + "  LET "
        + "      $maxYear = max(year)" + "  GROUP BY" + "  source");
    try {
      List<ODocument> results = db.query(sql);
      fail("Invalid query, usage of LET, aggregate functions and GROUP BY together is not supported");
    } catch (OCommandSQLParsingException x) {

    }
  }

  @Test
  public void testNullProjection() {
    OSQLSynchQuery sql = new OSQLSynchQuery("SELECT 1 AS integer, 'Test' AS string, NULL AS nothing, [] AS array, {} AS object");

    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 1);
    ODocument doc = results.get(0);
    assertEquals(doc.field("integer"), 1);
    assertEquals(doc.field("string"), "Test");
    assertNull(doc.field("nothing"));
    boolean nullFound = false;
    for (String s : doc.fieldNames()) {
      if (s.equals("nothing")) {
        nullFound = true;
        break;
      }
    }
    assertTrue(nullFound);

  }

  @Test
  public void testExpandSkipLimit() {
    // issue #4985
    OSQLSynchQuery sql = new OSQLSynchQuery(
        "SELECT expand(linked) from ExpandSkipLimit where parent = true order by nnum skip 1 limit 1");
    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 1);
    ODocument doc = results.get(0);
    assertEquals(doc.field("nnum"), 1);
  }

  @Test
  public void testMassiveOrderAscSkipLimit() {
    long begin = System.currentTimeMillis();
    int skip = 1000;
    OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from MassiveOrderSkipLimit order by nnum asc skip " + skip + " limit 5");
    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 5);
    System.out.println("elapsed: " + (System.currentTimeMillis() - begin));
    for (int i = 0; i < results.size(); i++) {
      ODocument doc = results.get(i);
      assertEquals(doc.field("nnum"), skip + i);
    }
  }

  @Test
  public void testMassiveOrderDescSkipLimit() {
    long begin = System.currentTimeMillis();
    int skip = 1000;
    OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from MassiveOrderSkipLimit order by nnum desc skip " + skip + " limit 5");

    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 5);
    System.out.println("elapsed: " + (System.currentTimeMillis() - begin));
    for (int i = 0; i < results.size(); i++) {
      ODocument doc = results.get(i);
      assertEquals(doc.field("nnum"), ORDER_SKIP_LIMIT_ITEMS - 1 - skip - i);
    }
  }

  @Test
  public void testBacktick() {
    OSQLSynchQuery sql = new OSQLSynchQuery("SELECT `foo-bar` as r from TestBacktick");
    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 1);
    ODocument doc = results.get(0);
    assertEquals(doc.field("r"), 10);
  }

  @Test
  public void testIntersectExpandLet() {
    //issue #5121
    OSQLSynchQuery sql = new OSQLSynchQuery("select expand(intersect($q1, $q2)) "
        + "let $q1 = (select from OUser where name ='admin')," + "$q2 = (select from OUser where name ='admin')");

    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 1);
    for (int i = 0; i < results.size(); i++) {
      ODocument doc = results.get(i);
      assertEquals(doc.field("name"), "admin");
    }
  }

  @Test
  public void testDatesListContainsString() {
    //issue #3526
    OSQLSynchQuery sql = new OSQLSynchQuery("select from OCommandExecutorSQLSelectTest_datesSet where foo contains '2015-10-21'");

    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 1);
  }

  @Test
  public void testParamWithMatches(){
    //issue #5229
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("param1", "adm.*");
    OSQLSynchQuery sql = new OSQLSynchQuery("select from OUser where name matches :param1");
    List<ODocument> results = db.query(sql, params);
    assertEquals(results.size(), 1);
  }

  @Test
  public void testParamWithMatchesQuoteRegex(){
    //issue #5229
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("param1", ".*admin[name].*");//will not work
    OSQLSynchQuery sql = new OSQLSynchQuery("select from matchesstuff where name matches :param1");
    List<ODocument> results = db.query(sql, params);
    assertEquals(results.size(), 0);
    params.put("param1", Pattern.quote("admin[name]") + ".*");//should work
    results = db.query(sql, params);
    assertEquals(results.size(), 1);
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
