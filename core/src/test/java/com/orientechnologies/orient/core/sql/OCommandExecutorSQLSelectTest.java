/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.Test;

public class OCommandExecutorSQLSelectTest extends BaseMemoryDatabase {

  private static int ORDER_SKIP_LIMIT_ITEMS = 100 * 1000;

  public void beforeTest() {
    super.beforeTest();
    getProfilerInstance().startRecording();

    db.command("CREATE class foo").close();
    db.command("CREATE property foo.name STRING").close();
    db.command("CREATE property foo.bar INTEGER").close();
    db.command("CREATE property foo.address EMBEDDED").close();
    db.command("CREATE property foo.comp STRING").close();
    db.command("CREATE property foo.osite INTEGER").close();

    db.command("CREATE index foo_name on foo (name) NOTUNIQUE").close();
    db.command("CREATE index foo_bar on foo (bar) NOTUNIQUE").close();
    db.command("CREATE index foo_comp_osite on foo (comp, osite) NOTUNIQUE").close();

    db.command(
            "insert into foo (name, bar, address) values ('a', 1, {'street':'1st street', 'city':'NY', '@type':'d'})")
        .close();
    db.command("insert into foo (name, bar) values ('b', 2)").close();
    db.command("insert into foo (name, bar) values ('c', 3)").close();

    db.command("insert into foo (comp, osite) values ('a', 1)").close();
    db.command("insert into foo (comp, osite) values ('b', 2)").close();

    db.command("CREATE class bar").close();

    db.command("insert into bar (name, foo) values ('a', 1)").close();
    db.command("insert into bar (name, foo) values ('b', 2)").close();
    db.command("insert into bar (name, foo) values ('c', 3)").close();
    db.command("insert into bar (name, foo) values ('d', 4)").close();
    db.command("insert into bar (name, foo) values ('e', 5)").close();
    db.command("insert into bar (name, foo) values ('f', 1)").close();
    db.command("insert into bar (name, foo) values ('g', 2)").close();
    db.command("insert into bar (name, foo) values ('h', 3)").close();
    db.command("insert into bar (name, foo) values ('i', 4)").close();
    db.command("insert into bar (name, foo) values ('j', 5)").close();
    db.command("insert into bar (name, foo) values ('k', 1)").close();
    db.command("insert into bar (name, foo) values ('l', 2)").close();
    db.command("insert into bar (name, foo) values ('m', 3)").close();
    db.command("insert into bar (name, foo) values ('n', 4)").close();
    db.command("insert into bar (name, foo) values ('o', 5)").close();

    db.command("CREATE class ridsorttest clusters 1").close();
    db.command("CREATE property ridsorttest.name INTEGER").close();
    db.command("CREATE index ridsorttest_name on ridsorttest (name) NOTUNIQUE").close();

    db.command("insert into ridsorttest (name) values (1)").close();
    db.command("insert into ridsorttest (name) values (5)").close();
    db.command("insert into ridsorttest (name) values (3)").close();
    db.command("insert into ridsorttest (name) values (4)").close();
    db.command("insert into ridsorttest (name) values (1)").close();
    db.command("insert into ridsorttest (name) values (8)").close();
    db.command("insert into ridsorttest (name) values (6)").close();

    db.command("CREATE class unwindtest").close();
    db.command("insert into unwindtest (name, coll) values ('foo', ['foo1', 'foo2'])").close();
    db.command("insert into unwindtest (name, coll) values ('bar', ['bar1', 'bar2'])").close();

    db.command("CREATE class unwindtest2").close();
    db.command("insert into unwindtest2 (name, coll) values ('foo', [])").close();

    db.command("CREATE class `edge`").close();

    db.command("CREATE class TestFromInSquare").close();
    db.command("insert into TestFromInSquare set tags = {' from ':'foo',' to ':'bar'}").close();

    db.command("CREATE class TestMultipleClusters").close();
    db.command("alter class TestMultipleClusters addcluster testmultipleclusters1 ").close();
    db.command("alter class TestMultipleClusters addcluster testmultipleclusters2 ").close();
    db.command("insert into cluster:testmultipleclusters set name = 'aaa'").close();
    db.command("insert into cluster:testmultipleclusters1 set name = 'foo'").close();
    db.command("insert into cluster:testmultipleclusters2 set name = 'bar'").close();

    db.command("CREATE class TestUrl").close();
    db.command("insert into TestUrl content { \"url\": \"http://www.google.com\" }").close();

    db.command("CREATE class TestParams").close();
    db.command("insert into TestParams  set name = 'foo', surname ='foo', active = true").close();
    db.command("insert into TestParams  set name = 'foo', surname ='bar', active = false").close();

    db.command("CREATE class TestParamsEmbedded").close();
    db.command(
            "insert into TestParamsEmbedded set emb = {  \n"
                + "            \"count\":0,\n"
                + "            \"testupdate\":\"1441258203385\"\n"
                + "         }")
        .close();
    db.command(
            "insert into TestParamsEmbedded set emb = {  \n"
                + "            \"count\":1,\n"
                + "            \"testupdate\":\"1441258203385\"\n"
                + "         }")
        .close();

    db.command("CREATE class TestBacktick").close();
    db.command("insert into TestBacktick  set foo = 1, bar = 2, `foo-bar` = 10").close();

    // /*** from issue #2743
    OSchema schema = db.getMetadata().getSchema();
    if (!schema.existsClass("alphabet")) {
      schema.createClass("alphabet", 1, null);
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

    db.command("create class OCommandExecutorSQLSelectTest_aggregations").close();
    db.command(
            "insert into OCommandExecutorSQLSelectTest_aggregations set data = [{\"size\": 0}, {\"size\": 0}, {\"size\": 30}, {\"size\": 50}, {\"size\": 50}]")
        .close();
  }

  private static void initCollateOnLinked(ODatabaseDocument db) {
    db.command("CREATE CLASS CollateOnLinked").close();
    db.command("CREATE CLASS CollateOnLinked2").close();
    db.command("CREATE PROPERTY CollateOnLinked.name String").close();
    db.command("ALTER PROPERTY CollateOnLinked.name collate ci").close();

    ODocument doc = new ODocument("CollateOnLinked");
    doc.field("name", "foo");
    doc.save();

    ODocument doc2 = new ODocument("CollateOnLinked2");
    doc2.field("linked", doc.getIdentity());
    doc2.save();
  }

  private static void initComplexFilterInSquareBrackets(ODatabaseDocument db) {
    db.command("CREATE CLASS ComplexFilterInSquareBrackets1").close();
    db.command("CREATE CLASS ComplexFilterInSquareBrackets2").close();
    db.command("INSERT INTO ComplexFilterInSquareBrackets1 SET name = 'n1', value = 1").close();
    db.command("INSERT INTO ComplexFilterInSquareBrackets1 SET name = 'n2', value = 2").close();
    db.command("INSERT INTO ComplexFilterInSquareBrackets1 SET name = 'n3', value = 3").close();
    db.command("INSERT INTO ComplexFilterInSquareBrackets1 SET name = 'n4', value = 4").close();
    db.command("INSERT INTO ComplexFilterInSquareBrackets1 SET name = 'n5', value = 5").close();
    db.command("INSERT INTO ComplexFilterInSquareBrackets1 SET name = 'n6', value = -1").close();
    db.command("INSERT INTO ComplexFilterInSquareBrackets1 SET name = 'n7', value = null").close();
    db.command(
            "INSERT INTO ComplexFilterInSquareBrackets2 SET collection = (select from ComplexFilterInSquareBrackets1)")
        .close();
  }

  private static void initFilterAndOrderByTest(ODatabaseDocument db) {
    db.command("CREATE CLASS FilterAndOrderByTest").close();
    db.command("CREATE PROPERTY FilterAndOrderByTest.dc DATETIME").close();
    db.command("CREATE PROPERTY FilterAndOrderByTest.active BOOLEAN").close();
    db.command(
            "CREATE INDEX FilterAndOrderByTest.active ON FilterAndOrderByTest (active) NOTUNIQUE")
        .close();

    db.command("insert into FilterAndOrderByTest SET dc = '2010-01-05 12:00:00:000', active = true")
        .close();
    db.command(
            "insert into FilterAndOrderByTest SET dc = '2010-05-05 14:00:00:000', active = false")
        .close();
    db.command("insert into FilterAndOrderByTest SET dc = '2009-05-05 16:00:00:000', active = true")
        .close();
    db.command(
            "insert into FilterAndOrderByTest SET dc = '2008-05-05 12:00:00:000', active = false")
        .close();
    db.command(
            "insert into FilterAndOrderByTest SET dc = '2014-05-05 14:00:00:000', active = false")
        .close();
    db.command("insert into FilterAndOrderByTest SET dc = '2016-01-05 14:00:00:000', active = true")
        .close();
  }

  private static void initMaxLongNumber(ODatabaseDocument db) {
    db.command("CREATE class MaxLongNumberTest").close();
    db.command("insert into MaxLongNumberTest set last = 1").close();
    db.command("insert into MaxLongNumberTest set last = null").close();
    db.command("insert into MaxLongNumberTest set last = 958769876987698").close();
    db.command("insert into MaxLongNumberTest set foo = 'bar'").close();
  }

  private static void initLinkListSequence(ODatabaseDocument db) {
    db.command("CREATE class LinkListSequence").close();

    db.command("insert into LinkListSequence set name = '1.1.1'").close();
    db.command("insert into LinkListSequence set name = '1.1.2'").close();
    db.command("insert into LinkListSequence set name = '1.2.1'").close();
    db.command("insert into LinkListSequence set name = '1.2.2'").close();
    db.command(
            "insert into LinkListSequence set name = '1.1', children = (select from LinkListSequence where name like '1.1.%')")
        .close();
    db.command(
            "insert into LinkListSequence set name = '1.2', children = (select from LinkListSequence where name like '1.2.%')")
        .close();
    db.command(
            "insert into LinkListSequence set name = '1', children = (select from LinkListSequence where name in ['1.1', '1.2'])")
        .close();
    db.command("insert into LinkListSequence set name = '2'").close();
    db.command(
            "insert into LinkListSequence set name = 'root', children = (select from LinkListSequence where name in ['1', '1'])")
        .close();
  }

  private static void initMatchesWithRegex(ODatabaseDocument db) {
    db.command("CREATE class matchesstuff").close();

    db.command("insert into matchesstuff (name, foo) values ('admin[name]', 1)").close();
  }

  private static void initDistinctLimit(ODatabaseDocument db) {
    db.command("CREATE class DistinctLimit").close();

    db.command("insert into DistinctLimit (name, foo) values ('one', 1)").close();
    db.command("insert into DistinctLimit (name, foo) values ('one', 1)").close();
    db.command("insert into DistinctLimit (name, foo) values ('two', 2)").close();
    db.command("insert into DistinctLimit (name, foo) values ('two', 2)").close();
  }

  private static void initDatesSet(ODatabaseDocument db) {
    db.command("create class OCommandExecutorSQLSelectTest_datesSet").close();
    db.command("create property OCommandExecutorSQLSelectTest_datesSet.foo embeddedlist date")
        .close();
    db.command("insert into OCommandExecutorSQLSelectTest_datesSet set foo = ['2015-10-21']")
        .close();
  }

  private static void initMassiveOrderSkipLimit(ODatabaseDocument db) {
    db.getMetadata().getSchema().createClass("MassiveOrderSkipLimit", 1, null);
    db.declareIntent(new OIntentMassiveInsert());
    String fieldValue =
        "laskdf lkajsd flaksjdf laksjd flakjsd flkasjd flkajsd flkajsd flkajsd flkajsd flkajsd flkjas;lkj a;ldskjf laksdj asdklasdjf lskdaj fladsd";
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

  private static void initExpandSkipLimit(ODatabaseDocument db) {
    db.command("create class ExpandSkipLimit clusters 1").close();

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

  private static OProfiler getProfilerInstance() {
    return Orient.instance().getProfiler();
  }

  @Test
  public void testUseIndexWithOrderBy2() throws Exception {
    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult =
        db.command(new OCommandSQL("select * from foo where address.city = 'NY' order by name ASC"))
            .execute();
    assertEquals(qResult.size(), 1);
  }

  @Test
  public void testUseIndexWithOr() throws Exception {
    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult =
        db.command(new OCommandSQL("select * from foo where bar = 2 or name ='a' and bar >= 0"))
            .execute();

    assertEquals(qResult.size(), 2);
    assertEquals(indexUsages(db), idxUsagesBefore + 2);
  }

  @Test
  public void testDoNotUseIndexWithOrNotIndexed() throws Exception {

    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult =
        db.command(new OCommandSQL("select * from foo where bar = 2 or notIndexed = 3")).execute();

    assertEquals(indexUsages(db), idxUsagesBefore);
  }

  @Test
  public void testCompositeIndex() {
    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult =
        db.command(new OCommandSQL("select * from foo where comp = 'a' and osite = 1")).execute();

    assertEquals(qResult.size(), 1);
    assertEquals(indexUsages(db), idxUsagesBefore + 1);
  }

  @Test
  public void testProjection() {
    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult =
        db.command(new OCommandSQL("select a from foo where name = 'a' or bar = 1")).execute();

    assertEquals(qResult.size(), 1);
    assertEquals(indexUsages(db), idxUsagesBefore + 2);
  }

  @Test
  public void testProjection2() {
    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult =
        db.command(new OCommandSQL("select a from foo where name = 'a' or bar = 2")).execute();

    assertEquals(qResult.size(), 2);
    assertEquals(indexUsages(db), idxUsagesBefore + 2);
  }

  @Test
  public void testCompositeIndex2() {
    long idxUsagesBefore = indexUsages(db);

    List<ODocument> qResult =
        db.command(
                new OCommandSQL("select * from foo where (comp = 'a' and osite = 1) or name = 'a'"))
            .execute();

    assertEquals(qResult.size(), 2);
    assertEquals(indexUsages(db), idxUsagesBefore + 2);
  }

  @Test
  public void testOperatorPriority() {

    List<OResult> qResult =
        db.command("select * from foo where name ='a' and bar = 1000 or name = 'b'").stream()
            .collect(Collectors.toList());

    List<OResult> qResult2 =
        db.command("select * from foo where name = 'b' or name ='a' and bar = 1000").stream()
            .collect(Collectors.toList());

    List<OResult> qResult3 =
        db.command("select * from foo where name = 'b' or (name ='a' and bar = 1000)").stream()
            .collect(Collectors.toList());

    List<OResult> qResult4 =
        db.command("select * from foo where (name ='a' and bar = 1000) or name = 'b'").stream()
            .collect(Collectors.toList());

    List<OResult> qResult5 =
        db.command("select * from foo where ((name ='a' and bar = 1000) or name = 'b')").stream()
            .collect(Collectors.toList());

    List<OResult> qResult6 =
        db.command("select * from foo where ((name ='a' and (bar = 1000)) or name = 'b')").stream()
            .collect(Collectors.toList());

    List<OResult> qResult7 =
        db.command("select * from foo where (((name ='a' and bar = 1000)) or name = 'b')").stream()
            .collect(Collectors.toList());

    List<OResult> qResult8 =
        db.command("select * from foo where (((name ='a' and bar = 1000)) or (name = 'b'))")
            .stream()
            .collect(Collectors.toList());

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
    List<OResult> qResult =
        db
            .command(
                "select * from bar where name ='a' and foo = 1 or name='b' or name='c' and foo = 3 and other = 4 or name = 'e' and foo = 5 or name = 'm' and foo > 2 ")
            .stream()
            .collect(Collectors.toList());

    List<OResult> qResult2 =
        db
            .command(
                "select * from bar where (name ='a' and foo = 1) or name='b' or (name='c' and foo = 3 and other = 4) or (name = 'e' and foo = 5) or (name = 'm' and foo > 2)")
            .stream()
            .collect(Collectors.toList());

    List<OResult> qResult3 =
        db
            .command(
                "select * from bar where (name ='a' and foo = 1) or (name='b') or (name='c' and foo = 3 and other = 4) or (name ='e' and foo = 5) or (name = 'm' and foo > 2)")
            .stream()
            .collect(Collectors.toList());

    List<OResult> qResult4 =
        db
            .command(
                "select * from bar where (name ='a' and foo = 1) or ((name='b') or (name='c' and foo = 3 and other = 4)) or (name = 'e' and foo = 5) or (name = 'm' and foo > 2)")
            .stream()
            .collect(Collectors.toList());

    List<OResult> qResult5 =
        db
            .command(
                "select * from bar where (name ='a' and foo = 1) or ((name='b') or (name='c' and foo = 3 and other = 4) or (name = 'e' and foo = 5)) or (name = 'm' and foo > 2)")
            .stream()
            .collect(Collectors.toList());

    assertEquals(qResult.size(), qResult2.size());
    assertEquals(qResult.size(), qResult3.size());
    assertEquals(qResult.size(), qResult4.size());
    assertEquals(qResult.size(), qResult5.size());
  }

  @Test
  public void testOperatorPriority3() {
    List<OResult> qResult =
        db
            .command(
                "select * from bar where name <> 'a' and foo = 1 or name='b' or name='c' and foo = 3 and other <> 4 or name = 'e' and foo = 5 or name = 'm' and foo > 2 ")
            .stream()
            .collect(Collectors.toList());

    List<OResult> qResult2 =
        db
            .command(
                "select * from bar where (name <> 'a' and foo = 1) or name='b' or (name='c' and foo = 3 and other <>  4) or (name = 'e' and foo = 5) or (name = 'm' and foo > 2)")
            .stream()
            .collect(Collectors.toList());

    List<OResult> qResult3 =
        db
            .command(
                "select * from bar where ( name <> 'a' and foo = 1) or (name='b') or (name='c' and foo = 3 and other <>  4) or (name ='e' and foo = 5) or (name = 'm' and foo > 2)")
            .stream()
            .collect(Collectors.toList());

    List<OResult> qResult4 =
        db
            .command(
                "select * from bar where (name <> 'a' and foo = 1) or ( (name='b') or (name='c' and foo = 3 and other <>  4)) or  (name = 'e' and foo = 5) or (name = 'm' and foo > 2)")
            .stream()
            .collect(Collectors.toList());

    List<OResult> qResult5 =
        db
            .command(
                "select * from bar where (name <> 'a' and foo = 1) or ((name='b') or (name='c' and foo = 3 and other <>  4) or (name = 'e' and foo = 5)) or (name = 'm' and foo > 2)")
            .stream()
            .collect(Collectors.toList());

    assertEquals(qResult.size(), qResult2.size());
    assertEquals(qResult.size(), qResult3.size());
    assertEquals(qResult.size(), qResult4.size());
    assertEquals(qResult.size(), qResult5.size());
  }

  @Test
  public void testExpandOnEmbedded() {
    try (OResultSet qResult = db.command("select expand(address) from foo where name = 'a'")) {
      assertEquals(qResult.next().getProperty("city"), "NY");
      assertFalse(qResult.hasNext());
    }
  }

  @Test
  public void testFlattenOnEmbedded() {
    List<ODocument> qResult =
        db.command(new OCommandSQL("select flatten(address) from foo where name = 'a'")).execute();

    assertEquals(qResult.size(), 1);
    assertEquals(qResult.get(0).field("city"), "NY");
  }

  @Test
  public void testLimit() {
    OResultSet qResult = db.query("select from foo limit 3");
    assertEquals(qResult.stream().count(), 3);
  }

  @Test
  public void testLimitWithMetadataQuery() {
    OResultSet qResult = db.query("select expand(classes) from metadata:schema limit 3");
    assertEquals(qResult.stream().count(), 3);
  }

  @Test
  public void testOrderByWithMetadataQuery() {
    OResultSet qResult = db.query("select expand(classes) from metadata:schema order by name");
    assertTrue(qResult.stream().count() > 0);
  }

  @Test
  public void testLimitWithUnnamedParam() {
    OResultSet qResult = db.query("select from foo limit ?", 3);
    assertEquals(qResult.stream().count(), 3);
  }

  @Test
  public void testLimitWithNamedParam() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("lim", 2);
    OResultSet qResult = db.command("select from foo limit :lim", params);
    assertEquals(qResult.stream().count(), 2);
  }

  @Test
  public void testLimitWithNamedParam2() {
    // issue #5493
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("limit", 2);
    OResultSet qResult = db.command("select from foo limit :limit", params);
    assertEquals(qResult.stream().count(), 2);
  }

  @Test
  public void testParamsInLetSubquery() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "foo");
    OResultSet qResult =
        db.command(
            "select from TestParams let $foo = (select name from TestParams where surname = :name) where surname in $foo.name ",
            params);
    assertEquals(qResult.stream().count(), 1);
  }

  @Test
  public void testBooleanParams() {
    // issue #4224
    OResultSet qResult =
        db.command("select name from TestParams where name = ? and active = ?", "foo", true);
    assertEquals(qResult.stream().count(), 1);
  }

  @Test
  public void testFromInSquareBrackets() {
    try (OResultSet qResult = db.command("select tags[' from '] as a from TestFromInSquare")) {
      assertEquals(qResult.next().getProperty("a"), "foo");
      assertFalse(qResult.hasNext());
    }
  }

  @Test
  public void testNewline() {
    OResultSet qResult = db.command("select\n1 as ACTIVE\nFROM foo");
    assertEquals(qResult.stream().count(), 5);
  }

  @Test
  public void testOrderByRid() {
    OResultSet qResult = db.query("select from ridsorttest order by @rid ASC");
    assertTrue(qResult.hasNext());

    OResult prev = qResult.next();
    while (qResult.hasNext()) {
      OResult next = qResult.next();
      assertTrue(prev.getIdentity().get().compareTo(next.getIdentity().get()) <= 0);
      prev = next;
    }
    qResult.close();

    qResult = db.query("select from ridsorttest order by @rid DESC");
    assertTrue(qResult.hasNext());

    prev = qResult.next();
    while (qResult.hasNext()) {
      OResult next = qResult.next();
      assertTrue(prev.getIdentity().get().compareTo(next.getIdentity().get()) >= 0);
      prev = next;
    }
    qResult.close();

    qResult = db.query("select from ridsorttest where name > 3 order by @rid DESC");
    assertTrue(qResult.hasNext());

    prev = qResult.next();
    while (qResult.hasNext()) {
      OResult next = qResult.next();
      assertTrue(prev.getIdentity().get().compareTo(next.getIdentity().get()) >= 0);
      prev = next;
    }
  }

  @Test
  public void testUnwind() {
    List<OResult> qResult =
        db.command("select from unwindtest unwind coll").stream().collect(Collectors.toList());

    assertEquals(qResult.size(), 4);
    for (OResult doc : qResult) {
      String name = doc.getProperty("name");
      String coll = doc.getProperty("coll");
      assertTrue(coll.startsWith(name));
      assertFalse(doc.getIdentity().isPresent());
    }
  }

  @Test
  public void testUnwind2() {
    List<OResult> qResult =
        db.command("select from unwindtest2 unwind coll").stream().collect(Collectors.toList());

    assertEquals(qResult.size(), 1);
    for (OResult doc : qResult) {
      Object coll = doc.getProperty("coll");
      assertNull(coll);
      assertFalse(doc.getIdentity().isPresent());
    }
  }

  @Test
  public void testUnwindOrder() {
    List<OResult> qResult =
        db.command("select from unwindtest order by coll unwind coll").stream()
            .collect(Collectors.toList());

    assertEquals(qResult.size(), 4);
    for (OResult doc : qResult) {
      String name = doc.getProperty("name");
      String coll = doc.getProperty("coll");
      assertTrue(coll.startsWith(name));
      assertFalse(doc.getIdentity().isPresent());
    }
  }

  @Test
  public void testUnwindSkip() {
    List<OResult> qResult =
        db.command("select from unwindtest unwind coll skip 1").stream()
            .collect(Collectors.toList());

    assertEquals(qResult.size(), 3);
    for (OResult doc : qResult) {
      String name = doc.getProperty("name");
      String coll = doc.getProperty("coll");
      assertTrue(coll.startsWith(name));
    }
  }

  @Test
  public void testUnwindLimit() {
    List<OResult> qResult =
        db.command("select from unwindtest unwind coll limit 1").stream()
            .collect(Collectors.toList());

    assertEquals(qResult.size(), 1);
    for (OResult doc : qResult) {
      String name = doc.getProperty("name");
      String coll = doc.getProperty("coll");
      assertTrue(coll.startsWith(name));
    }
  }

  @Test
  public void testUnwindLimit3() {
    List<OResult> qResult =
        db.command("select from unwindtest unwind coll limit 3").stream()
            .collect(Collectors.toList());
    ;

    assertEquals(qResult.size(), 3);
    for (OResult doc : qResult) {
      String name = doc.getProperty("name");
      String coll = doc.getProperty("coll");
      assertTrue(coll.startsWith(name));
    }
  }

  @Test
  public void testUnwindSkipAndLimit() {
    List<OResult> qResult =
        db.command("select from unwindtest unwind coll skip 1 limit 1").stream()
            .collect(Collectors.toList());

    assertEquals(qResult.size(), 1);
    for (OResult doc : qResult) {
      String name = doc.getProperty("name");
      String coll = doc.getProperty("coll");
      assertTrue(coll.startsWith(name));
    }
  }

  @Test
  public void testMultipleClusters() {
    OResultSet qResult = db.command("select from cluster:[testmultipleclusters1]");

    assertEquals(qResult.stream().count(), 1);

    qResult = db.command("select from cluster:[testmultipleclusters1, testmultipleclusters2]");

    assertEquals(qResult.stream().count(), 2);
  }

  @Test
  public void testMatches() {
    List<?> result =
        db.query(
            new OSQLSynchQuery<Object>(
                "select from foo where name matches '(?i)(^\\\\Qa\\\\E$)|(^\\\\Qname2\\\\E$)|(^\\\\Qname3\\\\E$)' and bar = 1"));
    assertEquals(result.size(), 1);
  }

  @Test
  public void testStarPosition() {
    List<OResult> result =
        db.query("select *, name as blabla from foo where name = 'a'").stream()
            .collect(Collectors.toList());

    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getProperty("blabla"), "a");

    result =
        db.query("select name as blabla, * from foo where name = 'a'").stream()
            .collect(Collectors.toList());

    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getProperty("blabla"), "a");

    result =
        db.query("select name as blabla, *, fff as zzz from foo where name = 'a'").stream()
            .collect(Collectors.toList());

    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getProperty("blabla"), "a");
  }

  @Test
  public void testQuotedClassName() {
    OResultSet qResult = db.query("select from `edge`");

    assertEquals(qResult.stream().count(), 0);
  }

  public void testUrl() {

    List<OResult> qResult = db.command("select from TestUrl").stream().collect(Collectors.toList());

    assertEquals(qResult.size(), 1);
    assertEquals(qResult.get(0).getProperty("url"), "http://www.google.com");
  }

  @Test
  public void testUnwindSkipAndLimit2() {
    List<OResult> qResult =
        db.command("select from unwindtest unwind coll skip 1 limit 2").stream()
            .collect(Collectors.toList());

    assertEquals(qResult.size(), 2);
    for (OResult doc : qResult) {
      String name = doc.getProperty("name");
      String coll = doc.getProperty("coll");
      assertTrue(coll.startsWith(name));
    }
  }

  @Test
  public void testMultipleParamsWithSameName() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("param1", "foo");
    OResultSet qResult =
        db.query("select from TestParams where name like '%' + :param1 + '%'", params);
    assertEquals(qResult.stream().count(), 2);

    qResult =
        db.query(
            "select from TestParams where name like '%' + :param1 + '%' and surname like '%' + :param1 + '%'",
            params);
    assertEquals(qResult.stream().count(), 1);

    params = new HashMap<String, Object>();
    params.put("param1", "bar");

    qResult = db.query("select from TestParams where surname like '%' + :param1 + '%'", params);
    assertEquals(qResult.stream().count(), 1);
  }

  // /*** from issue #2743
  @Test
  public void testBasicQueryOrdered() {
    OResultSet results = db.query("SELECT from alphabet ORDER BY letter");
    assertEquals(26, results.stream().count());
  }

  @Test
  public void testSkipZeroOrdered() {
    OResultSet results = db.query("SELECT from alphabet ORDER BY letter SKIP 0");
    assertEquals(26, results.stream().count());
  }

  @Test
  public void testSkipOrdered() {
    OResultSet results = db.query("SELECT from alphabet ORDER BY letter SKIP 7");
    assertEquals(19, results.stream().count());
  }

  @Test
  public void testLimitOrdered() {
    OResultSet results = db.query("SELECT from alphabet ORDER BY letter LIMIT 9");
    assertEquals(9, results.stream().count());
  }

  @Test
  public void testLimitMinusOneOrdered() {
    OResultSet results = db.query("SELECT from alphabet ORDER BY letter LIMIT -1");
    assertEquals(26, results.stream().count());
  }

  @Test
  public void testSkipAndLimitOrdered() {
    OResultSet results = db.query("SELECT from alphabet ORDER BY letter SKIP 7 LIMIT 9");
    assertEquals(9, results.stream().count());
  }

  @Test
  public void testSkipAndLimitMinusOneOrdered() {
    OResultSet results = db.query("SELECT from alphabet ORDER BY letter SKIP 7 LIMIT -1");
    assertEquals(19, results.stream().count());
  }

  @Test
  public void testLetAsListAsString() {
    String sql =
        "SELECT $ll as lll from unwindtest let $ll = coll.asList().asString() where name = 'bar'";
    List<OResult> results = db.query(sql).stream().collect(Collectors.toList());
    assertEquals(1, results.size());
    assertNotNull(results.get(0).getProperty("lll"));
    assertEquals("[bar1, bar2]", results.get(0).getProperty("lll"));
  }

  @Test
  public void testAggregations() {
    OSQLSynchQuery sql =
        new OSQLSynchQuery(
            "select data.size as collection_content, data.size() as collection_size, min(data.size) as collection_min, max(data.size) as collection_max, sum(data.size) as collection_sum, avg(data.size) as collection_avg from OCommandExecutorSQLSelectTest_aggregations");
    List<ODocument> results = db.query(sql);
    assertEquals(1, results.size());
    ODocument doc = results.get(0);

    assertThat(doc.<Integer>field("collection_size")).isEqualTo(5);
    assertThat(doc.<Integer>field("collection_sum")).isEqualTo(130);
    assertThat(doc.<Integer>field("collection_avg")).isEqualTo(26);
    assertThat(doc.<Integer>field("collection_min")).isEqualTo(0);
    assertThat(doc.<Integer>field("collection_max")).isEqualTo(50);

    //
    //    assertEquals(5, doc.field("collection_size"));
    //    assertEquals(130, doc.field("collection_sum"));
    //    assertEquals(26, doc.field("collection_avg"));
    //    assertEquals(0, doc.field("collection_min"));
    //    assertEquals(50, doc.field("collection_max"));
  }

  @Test
  public void testLetOrder() {
    OSQLSynchQuery sql =
        new OSQLSynchQuery(
            "SELECT"
                + "      source,"
                + "  $maxYear as maxYear"
                + "              FROM"
                + "      ("
                + "          SELECT expand( $union ) "
                + "  LET"
                + "      $a = (SELECT 'A' as source, 2013 as year),"
                + "  $b = (SELECT 'B' as source, 2012 as year),"
                + "  $union = unionAll($a,$b) "
                + "  ) "
                + "  LET "
                + "      $maxYear = max(year)"
                + "  GROUP BY"
                + "  source");
    try {
      List<ODocument> results = db.query(sql);
      fail(
          "Invalid query, usage of LET, aggregate functions and GROUP BY together is not supported");
    } catch (OCommandSQLParsingException x) {

    }
  }

  @Test
  public void testNullProjection() {
    String sql =
        "SELECT 1 AS integer, 'Test' AS string, NULL AS nothing, [] AS array, {} AS object";

    List<OResult> results = db.query(sql).stream().collect(Collectors.toList());
    assertEquals(results.size(), 1);
    OResult doc = results.get(0);
    assertThat(doc.<Integer>getProperty("integer")).isEqualTo(1);
    assertEquals(doc.getProperty("string"), "Test");
    assertNull(doc.getProperty("nothing"));
    boolean nullFound = false;
    for (String s : doc.getPropertyNames()) {
      if (s.equals("nothing")) {
        nullFound = true;
        break;
      }
    }
    assertTrue(nullFound);
  }

  @Test
  public void testExpandSkipLimit() {
    initExpandSkipLimit(db);
    // issue #4985
    OResultSet results =
        db.query(
            "SELECT expand(linked) from ExpandSkipLimit where parent = true order by nnum skip 1 limit 1");
    OResult doc = results.next();
    assertThat(doc.<Integer>getProperty("nnum")).isEqualTo(1);
  }

  @Test
  public void testBacktick() {
    OResultSet results = db.query("SELECT `foo-bar` as r from TestBacktick");
    OResult doc = results.next();
    assertThat(doc.<Integer>getProperty("r")).isEqualTo(10);
  }

  @Test
  public void testOrderByEmbeddedParams() {
    // issue #4949
    Map<String, Object> parameters = new HashMap<String, Object>();
    parameters.put("paramvalue", "count");
    OResultSet qResult =
        db.command("select from TestParamsEmbedded order by emb[:paramvalue] DESC", parameters);
    Map embedded = qResult.next().getProperty("emb");
    assertEquals(embedded.get("count"), 1);
    qResult.close();
  }

  @Test
  public void testOrderByEmbeddedParams2() {
    // issue #4949
    Map<String, Object> parameters = new HashMap<String, Object>();
    parameters.put("paramvalue", "count");
    OResultSet qResult =
        db.command("select from TestParamsEmbedded order by emb[:paramvalue] ASC", parameters);
    Map embedded = qResult.next().getProperty("emb");
    assertEquals(embedded.get("count"), 0);
    qResult.close();
    qResult =
        db.command("select from TestParamsEmbedded order by emb[:paramvalue] ASC", parameters);
    assertEquals(qResult.stream().count(), 2);
  }

  @Test
  public void testMassiveOrderAscSkipLimit() {
    initMassiveOrderSkipLimit(db);
    int skip = 1000;
    OResultSet results =
        db.query("SELECT from MassiveOrderSkipLimit order by nnum asc skip " + skip + " limit 5");

    int i = 0;
    while (results.hasNext()) {
      OResult doc = results.next();
      assertThat(doc.<Integer>getProperty("nnum")).isEqualTo(skip + i);
      i++;
    }
    assertEquals(i, 5);
  }

  @Test
  public void testMassiveOrderDescSkipLimit() {
    initMassiveOrderSkipLimit(db);
    int skip = 1000;
    OResultSet results =
        db.query("SELECT from MassiveOrderSkipLimit order by nnum desc skip " + skip + " limit 5");
    int i = 0;
    while (results.hasNext()) {
      OResult doc = results.next();
      assertThat(doc.<Integer>getProperty("nnum")).isEqualTo(ORDER_SKIP_LIMIT_ITEMS - 1 - skip - i);
      i++;
    }
    assertEquals(i, 5);
  }

  @Test
  public void testIntersectExpandLet() {
    // issue #5121
    OResultSet results =
        db.query(
            "select expand(intersect($q1, $q2)) "
                + "let $q1 = (select from OUser where name ='admin'),"
                + "$q2 = (select from OUser where name ='admin')");
    assertTrue(results.hasNext());
    OResult doc = results.next();
    assertEquals(doc.getProperty("name"), "admin");
    assertFalse(results.hasNext());
  }

  @Test
  public void testDatesListContainsString() {
    initDatesSet(db);
    // issue #3526

    OResultSet results =
        db.query(
            "select from OCommandExecutorSQLSelectTest_datesSet where foo contains '2015-10-21'");
    assertEquals(results.stream().count(), 1);
  }

  @Test
  public void testParamWithMatches() {
    // issue #5229
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("param1", "adm.*");
    OResultSet results = db.query("select from OUser where name matches :param1", params);
    assertEquals(results.stream().count(), 1);
  }

  @Test
  public void testParamWithMatchesQuoteRegex() {
    initMatchesWithRegex(db);
    // issue #5229
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("param1", ".*admin[name].*"); // will not work
    OResultSet results = db.query("select from matchesstuff where name matches :param1", params);
    assertEquals(results.stream().count(), 0);
    params.put("param1", Pattern.quote("admin[name]") + ".*"); // should work
    results = db.query("select from matchesstuff where name matches :param1", params);
    assertEquals(results.stream().count(), 1);
  }

  @Test
  public void testMatchesWithQuotes() {
    initMatchesWithRegex(db);
    // issue #5229
    String pattern = Pattern.quote("adm") + ".*";
    OResultSet results = db.query("SELECT FROM matchesstuff WHERE (name matches ?)", pattern);
    assertEquals(results.stream().count(), 1);
  }

  @Test
  public void testMatchesWithQuotes2() {
    initMatchesWithRegex(db);
    // issue #5229
    OResultSet results =
        db.query(
            "SELECT FROM matchesstuff WHERE (name matches '\\\\Qadm\\\\E.*' and not ( name matches '(.*)foo(.*)' ) )");
    assertEquals(results.stream().count(), 1);
  }

  @Test
  public void testMatchesWithQuotes3() {
    initMatchesWithRegex(db);
    // issue #5229
    OResultSet results =
        db.query(
            "SELECT FROM matchesstuff WHERE (name matches '\\\\Qadm\\\\E.*' and  ( name matches '\\\\Qadmin\\\\E.*' ) )");
    assertEquals(results.stream().count(), 1);
  }

  @Test
  public void testParamWithMatchesAndNot() {
    // issue #5229
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("param1", "adm.*");
    params.put("param2", "foo.*");
    OResultSet results =
        db.query(
            "select from OUser where (name matches :param1 and not (name matches :param2))",
            params);
    assertEquals(results.stream().count(), 1);

    params.put("param1", Pattern.quote("adm") + ".*");
    results =
        db.query(
            "select from OUser where (name matches :param1 and not (name matches :param2))",
            params);
    assertEquals(results.stream().count(), 1);
  }

  @Test
  public void testDistinctLimit() {
    initDistinctLimit(db);
    OResultSet results = db.query("select distinct(name) from DistinctLimit limit 1");
    assertEquals(results.stream().count(), 1);

    results = db.query("select distinct(name) from DistinctLimit limit 2");
    assertEquals(results.stream().count(), 2);

    results = db.query("select distinct(name) from DistinctLimit limit 3");
    assertEquals(results.stream().count(), 2);

    results = db.query("select distinct(name) from DistinctLimit limit -1");
    assertEquals(results.stream().count(), 2);
  }

  @Test
  public void testSelectFromClusterNumber() {
    initDistinctLimit(db);
    OClass clazz = db.getMetadata().getSchema().getClass("DistinctLimit");
    int clusterId = clazz.getClusterIds()[0];
    OResultSet results = db.query("select from cluster:" + clusterId + " limit 1");
    assertEquals(results.stream().count(), 1);
  }

  @Test
  public void testLinkListSequence1() {
    initLinkListSequence(db);
    OSQLSynchQuery sql =
        new OSQLSynchQuery(
            "select expand(children.children.children) from LinkListSequence where name = 'root'");
    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 4);
    for (ODocument result : results) {
      String value = result.field("name");
      assertEquals(value.length(), 5);
    }
  }

  @Test
  public void testLinkListSequence2() {
    initLinkListSequence(db);
    OSQLSynchQuery sql =
        new OSQLSynchQuery(
            "select expand(children[0].children.children) from LinkListSequence where name = 'root'");
    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 4);
    for (ODocument result : results) {
      String value = result.field("name");
      assertEquals(value.length(), 5);
    }
  }

  @Test
  public void testLinkListSequence3() {
    initLinkListSequence(db);
    String sql =
        "select expand(children[0].children[0].children) from LinkListSequence where name = 'root'";
    List<OResult> results = db.query(sql).stream().collect(Collectors.toList());
    assertEquals(results.size(), 2);
    for (OResult result : results) {
      String value = result.getProperty("name");
      assertTrue(value.equals("1.1.1") || value.equals("1.1.2"));
    }
  }

  @Test
  public void testMaxLongNumber() {
    initMaxLongNumber(db);
    // issue #5664
    OResultSet results = db.query("select from MaxLongNumberTest WHERE last < 10 OR last is null");
    assertEquals(results.stream().count(), 3);
    db.command("update MaxLongNumberTest set last = max(91,ifnull(last,0))").close();
    results = db.query("select from MaxLongNumberTest WHERE last < 10 OR last is null");
    assertEquals(results.stream().count(), 0);
  }

  @Test
  public void testFilterAndOrderBy() {
    initFilterAndOrderByTest(db);
    // issue http://www.prjhub.com/#/issues/6199

    String sql = "SELECT FROM FilterAndOrderByTest WHERE active = true ORDER BY dc DESC";
    List<OResult> results = db.query(sql).stream().collect(Collectors.toList());
    assertEquals(results.size(), 3);

    Calendar cal = new GregorianCalendar();

    Date date = results.get(0).getProperty("dc");
    cal.setTime(date);
    assertEquals(cal.get(Calendar.YEAR), 2016);

    date = results.get(1).getProperty("dc");
    cal.setTime(date);
    assertEquals(cal.get(Calendar.YEAR), 2010);

    date = results.get(2).getProperty("dc");
    cal.setTime(date);
    assertEquals(cal.get(Calendar.YEAR), 2009);
  }

  @Test
  public void testComplexFilterInSquareBrackets() {
    initComplexFilterInSquareBrackets(db);
    // issues #513 #5451

    String sql = "SELECT expand(collection[name = 'n1']) FROM ComplexFilterInSquareBrackets2";
    List<OResult> results = db.query(sql).stream().collect(Collectors.toList());
    assertEquals(results.size(), 1);
    assertEquals(results.iterator().next().getProperty("name"), "n1");

    sql =
        "SELECT expand(collection[name = 'n1' and value = 1]) FROM ComplexFilterInSquareBrackets2";
    results = db.query(sql).stream().collect(Collectors.toList());
    assertEquals(results.size(), 1);
    assertEquals(results.iterator().next().getProperty("name"), "n1");

    sql =
        "SELECT expand(collection[name = 'n1' and value > 1]) FROM ComplexFilterInSquareBrackets2";
    results = db.query(sql).stream().collect(Collectors.toList());
    assertEquals(results.size(), 0);

    sql =
        "SELECT expand(collection[name = 'n1' or value = -1]) FROM ComplexFilterInSquareBrackets2";
    results = db.query(sql).stream().collect(Collectors.toList());
    assertEquals(results.size(), 2);
    for (OResult doc : results) {
      assertTrue(doc.getProperty("name").equals("n1") || doc.getProperty("value").equals(-1));
    }

    sql =
        "SELECT expand(collection[name = 'n1' and not value = 1]) FROM ComplexFilterInSquareBrackets2";
    results = db.query(sql).stream().collect(Collectors.toList());
    assertEquals(results.size(), 0);

    sql = "SELECT expand(collection[value < 0]) FROM ComplexFilterInSquareBrackets2";
    results = db.query(sql).stream().collect(Collectors.toList());
    assertEquals(results.size(), 1);
    //    assertEquals(results.iterator().next().field("value"), -1);
    assertThat(results.iterator().next().<Integer>getProperty("value")).isEqualTo(-1);

    sql = "SELECT expand(collection[2]) FROM ComplexFilterInSquareBrackets2";
    results = db.query(sql).stream().collect(Collectors.toList());
    assertEquals(results.size(), 1);
  }

  @Test
  public void testCollateOnCollections() {
    // issue #4851
    db.command("create class OCommandExecutorSqlSelectTest_collateOnCollections").close();
    db.command(
            "create property OCommandExecutorSqlSelectTest_collateOnCollections.categories EMBEDDEDLIST string")
        .close();
    db.command(
            "insert into OCommandExecutorSqlSelectTest_collateOnCollections set categories=['a','b']")
        .close();
    db.command(
            "alter property OCommandExecutorSqlSelectTest_collateOnCollections.categories COLLATE ci")
        .close();
    db.command(
            "insert into OCommandExecutorSqlSelectTest_collateOnCollections set categories=['Math','English']")
        .close();
    db.command(
            "insert into OCommandExecutorSqlSelectTest_collateOnCollections set categories=['a','b','c']")
        .close();
    List<ODocument> results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select from OCommandExecutorSqlSelectTest_collateOnCollections where 'Math' in categories"));
    assertEquals(results.size(), 1);
    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select from OCommandExecutorSqlSelectTest_collateOnCollections where 'math' in categories"));
    assertEquals(results.size(), 1);
  }

  @Test
  public void testCountUniqueIndex() {
    // issue http://www.prjhub.com/#/issues/6419
    db.command("create class OCommandExecutorSqlSelectTest_testCountUniqueIndex").close();
    db.command("create property OCommandExecutorSqlSelectTest_testCountUniqueIndex.AAA String")
        .close();
    db.command(
            "create index OCommandExecutorSqlSelectTest_testCountUniqueIndex.AAA on OCommandExecutorSqlSelectTest_testCountUniqueIndex(AAA) unique")
        .close();

    List<OResult> results =
        db
            .query(
                "select count(*) as count from OCommandExecutorSqlSelectTest_testCountUniqueIndex where AAA='missing'")
            .stream()
            .collect(Collectors.toList());
    assertEquals(results.size(), 1);
    //    assertEquals(results.iterator().next().field("count"), 0l);

    assertThat(results.iterator().next().<Long>getProperty("count")).isEqualTo(0l);
  }

  @Test
  public void testEvalLong() {
    // http://www.prjhub.com/#/issues/6472
    List<OResult> results =
        db.query("SELECT EVAL(\"86400000 * 26\") AS value").stream().collect(Collectors.toList());
    assertEquals(results.size(), 1);

    //    assertEquals(results.get(0).field("value"), 86400000l * 26);
    assertThat(results.get(0).<Long>getProperty("value")).isEqualTo(86400000l * 26);
  }

  @Test
  public void testCollateOnLinked() {
    initCollateOnLinked(db);

    List<ODocument> results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select from CollateOnLinked2 where linked.name = 'foo' "));
    assertEquals(results.size(), 1);
    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select from CollateOnLinked2 where linked.name = 'FOO' "));
    assertEquals(results.size(), 1);
  }

  @Test
  public void testParamConcat() {
    // issue #6049
    OResultSet results = db.query("select from TestParams where surname like ? + '%'", "fo");
    assertEquals(results.stream().count(), 1);
  }

  @Test
  public void testCompositeIndexWithoutNullValues() {
    db.command("create class CompositeIndexWithoutNullValues").close();
    db.command("create property CompositeIndexWithoutNullValues.one String").close();
    db.command("create property CompositeIndexWithoutNullValues.two String").close();
    db.command(
            "create index CompositeIndexWithoutNullValues.one_two on CompositeIndexWithoutNullValues (one, two) NOTUNIQUE METADATA {ignoreNullValues: true}")
        .close();

    db.command("insert into CompositeIndexWithoutNullValues set one = 'foo'").close();
    db.command("insert into CompositeIndexWithoutNullValues set one = 'foo', two = 'bar'").close();
    List<ODocument> results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select from CompositeIndexWithoutNullValues where one = ?"),
            "foo");
    assertEquals(results.size(), 2);
    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select from CompositeIndexWithoutNullValues where one = ? and two = ?"),
            "foo",
            "bar");
    assertEquals(results.size(), 1);

    db.command("create class CompositeIndexWithoutNullValues2").close();
    db.command("create property CompositeIndexWithoutNullValues2.one String").close();
    db.command("create property CompositeIndexWithoutNullValues2.two String").close();
    db.command(
            "create index CompositeIndexWithoutNullValues2.one_two on CompositeIndexWithoutNullValues2 (one, two) NOTUNIQUE METADATA {ignoreNullValues: false}")
        .close();

    db.command("insert into CompositeIndexWithoutNullValues2 set one = 'foo'").close();
    db.command("insert into CompositeIndexWithoutNullValues2 set one = 'foo', two = 'bar'").close();
    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select from CompositeIndexWithoutNullValues2 where one = ?"),
            "foo");
    assertEquals(results.size(), 2);
    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select from CompositeIndexWithoutNullValues where one = ? and two = ?"),
            "foo",
            "bar");
    assertEquals(results.size(), 1);
  }

  @Test
  public void testDateFormat() {
    List<OResult> results =
        db.query("select date('2015-07-20', 'yyyy-MM-dd').format('dd.MM.yyyy') as dd").stream()
            .collect(Collectors.toList());
    assertEquals(results.size(), 1);
    assertEquals(results.get(0).getProperty("dd"), "20.07.2015");
  }

  @Test
  public void testConcatenateNamedParams() {
    // issue #5572
    OResultSet results =
        db.query("select from TestMultipleClusters where name like :p1 + '%'", "fo");
    assertEquals(results.stream().count(), 1);

    results = db.query("select from TestMultipleClusters where name like :p1 ", "fo");
    assertEquals(results.stream().count(), 0);
  }

  @Test
  public void testMethodsOnStrings() {
    // issue #5671
    List<OResult> results =
        db.query("select '1'.asLong() as long").stream().collect(Collectors.toList());
    assertEquals(results.size(), 1);
    //    assertEquals(results.get(0).field("long"), 1L);
    assertThat(results.get(0).<Long>getProperty("long")).isEqualTo(1L);
  }

  @Test
  public void testDifferenceOfInlineCollections() {
    // issue #5294
    List<OResult> results =
        db.query("select difference([1,2,3],[1,2]) as difference").stream()
            .collect(Collectors.toList());
    assertEquals(results.size(), 1);
    Object differenceFieldValue = results.get(0).getProperty("difference");
    assertTrue(differenceFieldValue instanceof Collection);
    assertEquals(((Collection) differenceFieldValue).size(), 1);
    assertEquals(((Collection) differenceFieldValue).iterator().next(), 3);
  }

  @Test
  public void testFoo() {
    // dispose it!
    db.command("create class testFoo");
    db.command("insert into testFoo set val = 1, name = 'foo'");
    db.command("insert into testFoo set val = 3, name = 'foo'");
    db.command("insert into testFoo set val = 5, name = 'bar'");

    OResultSet results = db.query("select sum(val), name from testFoo group by name");
    assertEquals(results.stream().count(), 2);
  }

  @Test
  public void testDateComparison() {
    // issue #6389

    db.command("create class TestDateComparison").close();
    db.command("create property TestDateComparison.dateProp DATE").close();

    db.command("insert into TestDateComparison set dateProp = '2016-05-01'").close();

    OResultSet results = db.query("SELECT from TestDateComparison WHERE dateProp >= '2016-05-01'");

    assertEquals(results.stream().count(), 1);
    results = db.query("SELECT from TestDateComparison WHERE dateProp <= '2016-05-01'");

    assertEquals(results.stream().count(), 1);
  }

  @Test
  public void testOrderByRidDescMultiCluster() {
    // issue #6694

    OClass clazz = db.getMetadata().getSchema().createClass("TestOrderByRidDescMultiCluster");
    if (clazz.getClusterIds().length < 2) {
      clazz.addCluster("TestOrderByRidDescMultiCluster_11111");
    }
    for (int i = 0; i < 100; i++) {
      db.command("insert into TestOrderByRidDescMultiCluster set foo = " + i).close();
    }

    List<OResult> results =
        db.query("SELECT from TestOrderByRidDescMultiCluster order by @rid desc").stream()
            .collect(Collectors.toList());
    assertEquals(results.size(), 100);
    OResult lastDoc = null;
    for (OResult doc : results) {
      if (lastDoc != null) {
        assertTrue(doc.getIdentity().get().compareTo(lastDoc.getIdentity().get()) < 0);
      }
      lastDoc = doc;
    }

    results =
        db.query("SELECT from TestOrderByRidDescMultiCluster order by @rid asc").stream()
            .collect(Collectors.toList());
    assertEquals(results.size(), 100);
    lastDoc = null;
    for (OResult doc : results) {
      if (lastDoc != null) {
        assertTrue(doc.getIdentity().get().compareTo(lastDoc.getIdentity().get()) > 0);
      }
      lastDoc = doc;
    }
  }

  @Test
  public void testCountOnSubclassIndexes() {
    // issue #6737

    db.command("create class testCountOnSubclassIndexes_superclass").close();
    db.command("create property testCountOnSubclassIndexes_superclass.foo boolean").close();
    db.command(
            "create index testCountOnSubclassIndexes_superclass.foo on testCountOnSubclassIndexes_superclass (foo) notunique")
        .close();

    db.command(
            "create class testCountOnSubclassIndexes_sub1 extends testCountOnSubclassIndexes_superclass")
        .close();
    db.command(
            "create index testCountOnSubclassIndexes_sub1.foo on testCountOnSubclassIndexes_sub1 (foo) notunique")
        .close();

    db.command(
            "create class testCountOnSubclassIndexes_sub2 extends testCountOnSubclassIndexes_superclass")
        .close();
    db.command(
            "create index testCountOnSubclassIndexes_sub2.foo on testCountOnSubclassIndexes_sub2 (foo) notunique")
        .close();

    db.command("insert into testCountOnSubclassIndexes_sub1 set name = 'a', foo = true").close();
    db.command("insert into testCountOnSubclassIndexes_sub1 set name = 'b', foo = false").close();
    db.command("insert into testCountOnSubclassIndexes_sub2 set name = 'c', foo = true").close();
    db.command("insert into testCountOnSubclassIndexes_sub2 set name = 'd', foo = true").close();
    db.command("insert into testCountOnSubclassIndexes_sub2 set name = 'e', foo = false").close();

    List<OResult> results =
        db.query("SELECT count(*) as count from testCountOnSubclassIndexes_sub1 where foo = true")
            .stream()
            .collect(Collectors.toList());
    assertEquals(results.size(), 1);
    assertEquals(results.get(0).getProperty("count"), (Object) 1L);

    results =
        db.query("SELECT count(*) as count from testCountOnSubclassIndexes_sub2 where foo = true")
            .stream()
            .collect(Collectors.toList());
    assertEquals(results.size(), 1);
    assertEquals(results.get(0).getProperty("count"), (Object) 2L);

    results =
        db
            .query(
                "SELECT count(*) as count from testCountOnSubclassIndexes_superclass where foo = true")
            .stream()
            .collect(Collectors.toList());
    assertEquals(results.size(), 1);
    assertEquals(results.get(0).getProperty("count"), (Object) 3L);
  }

  @Test
  public void testDoubleExponentNotation() {
    // issue #7013

    List<OResult> results = db.query("select 1e-2 as a").stream().collect(Collectors.toList());
    assertEquals(results.size(), 1);
    assertEquals(results.get(0).getProperty("a"), (Object) 0.01f);
  }

  @Test
  public void testConvertDouble() {
    // issue #7234

    db.command("create class testConvertDouble").close();
    db.command("insert into testConvertDouble set num = 100000").close();

    OResultSet results =
        db.query("SELECT FROM testConvertDouble WHERE num >= 50000 AND num <=300000000");

    assertEquals(results.stream().count(), 1);
  }

  @Test
  public void testFilterListsOfMaps() {

    String className = "testFilterListaOfMaps";

    db.command("create class " + className).close();
    db.command("create property " + className + ".tagz embeddedmap").close();
    db.command("insert into " + className + " set tagz = {}").close();
    db.command(
            "update "
                + className
                + " SET tagz.foo = [{name:'a', surname:'b'}, {name:'c', surname:'d'}]")
        .close();

    List<ODocument> results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select tagz.values()[0][name = 'a'] as t from " + className));
    assertEquals(results.size(), 1);
    Map map = results.get(0).field("t");
    assertEquals(map.get("surname"), "b");
  }

  @Test
  public void testComparisonOfShorts() {
    // issue #7578
    String className = "testComparisonOfShorts";
    db.command("create class " + className).close();
    db.command("create property " + className + ".state Short").close();
    db.command("INSERT INTO " + className + " set state = 1").close();
    db.command("INSERT INTO " + className + " set state = 1").close();
    db.command("INSERT INTO " + className + " set state = 2").close();

    OResultSet results = db.query("select from " + className + " where state in [1]");
    assertEquals(results.stream().count(), 2);

    results = db.query("select from " + className + " where [1] contains state");

    assertEquals(results.stream().count(), 2);
  }

  @Test
  public void testEnumAsParams() {
    // issue #7418
    String className = "testEnumAsParams";
    db.command("create class " + className).close();
    db.command("INSERT INTO " + className + " set status = ?", OType.STRING).close();
    db.command("INSERT INTO " + className + " set status = ?", OType.ANY).close();
    db.command("INSERT INTO " + className + " set status = ?", OType.BYTE).close();

    Map<String, Object> params = new HashMap<String, Object>();
    List enums = new ArrayList();
    enums.add(OType.STRING);
    enums.add(OType.BYTE);
    params.put("status", enums);
    OResultSet results = db.query("select from " + className + " where status in :status", params);
    assertEquals(results.stream().count(), 2);
  }

  @Test
  public void testEmbeddedMapOfMapsContainsValue() {
    // issue #7793
    String className = "testEmbeddedMapOfMapsContainsValue";

    db.command("create class " + className).close();
    db.command("create property " + className + ".embedded_map EMBEDDEDMAP").close();
    db.command("create property " + className + ".id INTEGER").close();
    db.command(
            "INSERT INTO "
                + className
                + " SET id = 0, embedded_map = {\"key_2\" : {\"name\" : \"key_2\", \"id\" : \"0\"}}")
        .close();
    db.command(
            "INSERT INTO "
                + className
                + " SET id = 1, embedded_map = {\"key_1\" : {\"name\" : \"key_1\", \"id\" : \"1\" }}")
        .close();

    OResultSet results =
        db.query(
            "select from "
                + className
                + " where embedded_map CONTAINSVALUE {\"name\":\"key_2\", \"id\":\"0\"}");
    assertEquals(results.stream().count(), 1);
  }

  @Test
  public void testInvertedIndexedCondition() {
    // issue #7820
    String className = "testInvertedIndexedCondition";

    db.command("create class " + className).close();
    db.command("create property " + className + ".name STRING").close();
    db.command("insert into " + className + " SET name = \"1\"").close();
    db.command("insert into " + className + " SET name = \"2\"").close();

    OResultSet results = db.query("SELECT * FROM " + className + " WHERE name >= \"0\"");
    assertEquals(results.stream().count(), 2);

    results = db.query("SELECT * FROM " + className + " WHERE \"0\" <= name");
    assertEquals(results.stream().count(), 2);

    db.command("CREATE INDEX " + className + ".name on " + className + " (name) UNIQUE").close();

    results = db.query("SELECT * FROM " + className + " WHERE \"0\" <= name");
    assertEquals(results.stream().count(), 2);

    results = db.query("SELECT * FROM " + className + " WHERE \"2\" <= name");
    assertEquals(results.stream().count(), 1);

    results = db.query("SELECT * FROM " + className + " WHERE name >= \"0\"");
    assertEquals(results.stream().count(), 2);
  }

  @Test
  public void testIsDefinedOnNull() {
    // issue #7879
    String className = "testIsDefinedOnNull";

    db.command("create class " + className).close();
    db.command("create property " + className + ".name STRING").close();
    db.command("insert into " + className + " SET name = null, x = 1").close();
    db.command("insert into " + className + " SET x = 2").close();

    OResultSet results = db.query("SELECT * FROM " + className + " WHERE name is defined");
    assertEquals((int) results.next().getProperty("x"), 1);
    results.close();
    results = db.query("SELECT * FROM " + className + " WHERE name is not defined");

    assertEquals((int) results.next().getProperty("x"), 2);
    results.close();
  }

  private long indexUsages(ODatabaseDocument db) {
    final long oldIndexUsage;
    try {
      oldIndexUsage = getProfilerInstance().getCounter("db." + db.getName() + ".query.indexUsed");
      return oldIndexUsage == -1 ? 0 : oldIndexUsage;
    } catch (Exception e) {
      fail();
    }
    return -1l;
  }
}
