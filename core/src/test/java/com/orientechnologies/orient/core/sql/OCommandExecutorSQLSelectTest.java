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

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class OCommandExecutorSQLSelectTest {
  static ODatabaseDocumentTx db;
  private static String DB_STORAGE = "memory";
  private static String DB_NAME = "OCommandExecutorSQLSelectTest";
  private static int ORDER_SKIP_LIMIT_ITEMS = 100 * 1000;

  @BeforeClass
  public static void beforeClass() throws Exception {
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
    db.command(new OCommandSQL("CREATE index foo_comp_osite on foo (comp, osite) NOTUNIQUE"))
        .execute();

    db.command(
            new OCommandSQL(
                "insert into foo (name, bar, address) values ('a', 1, {'street':'1st street', 'city':'NY', '@type':'d'})"))
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

    db.command(new OCommandSQL("CREATE class ridsorttest clusters 1")).execute();
    db.command(new OCommandSQL("CREATE property ridsorttest.name INTEGER")).execute();
    db.command(new OCommandSQL("CREATE index ridsorttest_name on ridsorttest (name) NOTUNIQUE"))
        .execute();

    db.command(new OCommandSQL("insert into ridsorttest (name) values (1)")).execute();
    db.command(new OCommandSQL("insert into ridsorttest (name) values (5)")).execute();
    db.command(new OCommandSQL("insert into ridsorttest (name) values (3)")).execute();
    db.command(new OCommandSQL("insert into ridsorttest (name) values (4)")).execute();
    db.command(new OCommandSQL("insert into ridsorttest (name) values (1)")).execute();
    db.command(new OCommandSQL("insert into ridsorttest (name) values (8)")).execute();
    db.command(new OCommandSQL("insert into ridsorttest (name) values (6)")).execute();

    db.command(new OCommandSQL("CREATE class unwindtest")).execute();
    db.command(
            new OCommandSQL("insert into unwindtest (name, coll) values ('foo', ['foo1', 'foo2'])"))
        .execute();
    db.command(
            new OCommandSQL("insert into unwindtest (name, coll) values ('bar', ['bar1', 'bar2'])"))
        .execute();

    db.command(new OCommandSQL("CREATE class unwindtest2")).execute();
    db.command(new OCommandSQL("insert into unwindtest2 (name, coll) values ('foo', [])"))
        .execute();

    db.command(new OCommandSQL("CREATE class `edge`")).execute();

    db.command(new OCommandSQL("CREATE class TestFromInSquare")).execute();
    db.command(
            new OCommandSQL(
                "insert into TestFromInSquare set tags = {' from ':'foo',' to ':'bar'}"))
        .execute();

    db.command(new OCommandSQL("CREATE class TestMultipleClusters")).execute();
    db.command(
            new OCommandSQL("alter class TestMultipleClusters addcluster testmultipleclusters1 "))
        .execute();
    db.command(
            new OCommandSQL("alter class TestMultipleClusters addcluster testmultipleclusters2 "))
        .execute();
    db.command(new OCommandSQL("insert into cluster:testmultipleclusters set name = 'aaa'"))
        .execute();
    db.command(new OCommandSQL("insert into cluster:testmultipleclusters1 set name = 'foo'"))
        .execute();
    db.command(new OCommandSQL("insert into cluster:testmultipleclusters2 set name = 'bar'"))
        .execute();

    db.command(new OCommandSQL("CREATE class TestUrl")).execute();
    db.command(
            new OCommandSQL("insert into TestUrl content { \"url\": \"http://www.google.com\" }"))
        .execute();

    db.command(new OCommandSQL("CREATE class TestParams")).execute();
    db.command(
            new OCommandSQL(
                "insert into TestParams  set name = 'foo', surname ='foo', active = true"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into TestParams  set name = 'foo', surname ='bar', active = false"))
        .execute();

    db.command(new OCommandSQL("CREATE class TestParamsEmbedded")).execute();
    db.command(
            new OCommandSQL(
                "insert into TestParamsEmbedded set emb = {  \n"
                    + "            \"count\":0,\n"
                    + "            \"testupdate\":\"1441258203385\"\n"
                    + "         }"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into TestParamsEmbedded set emb = {  \n"
                    + "            \"count\":1,\n"
                    + "            \"testupdate\":\"1441258203385\"\n"
                    + "         }"))
        .execute();

    db.command(new OCommandSQL("CREATE class TestBacktick")).execute();
    db.command(new OCommandSQL("insert into TestBacktick  set foo = 1, bar = 2, `foo-bar` = 10"))
        .execute();

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

    db.command(new OCommandSQL("create class OCommandExecutorSQLSelectTest_aggregations"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into OCommandExecutorSQLSelectTest_aggregations set data = [{\"size\": 0}, {\"size\": 0}, {\"size\": 30}, {\"size\": 50}, {\"size\": 50}]"))
        .execute();

    initExpandSkipLimit(db);
    initMassiveOrderSkipLimit(db);
    initDatesSet(db);

    initMatchesWithRegex(db);
    initDistinctLimit(db);
    initLinkListSequence(db);
    initMaxLongNumber(db);
    initFilterAndOrderByTest(db);
    initComplexFilterInSquareBrackets(db);
    initCollateOnLinked(db);
  }

  private static void initCollateOnLinked(ODatabaseDocumentTx db) {
    db.command(new OCommandSQL("CREATE CLASS CollateOnLinked")).execute();
    db.command(new OCommandSQL("CREATE CLASS CollateOnLinked2")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY CollateOnLinked.name String")).execute();
    db.command(new OCommandSQL("ALTER PROPERTY CollateOnLinked.name collate ci")).execute();

    ODocument doc = new ODocument("CollateOnLinked");
    doc.field("name", "foo");
    doc.save();

    ODocument doc2 = new ODocument("CollateOnLinked2");
    doc2.field("linked", doc.getIdentity());
    doc2.save();
  }

  private static void initComplexFilterInSquareBrackets(ODatabaseDocumentTx db) {
    db.command(new OCommandSQL("CREATE CLASS ComplexFilterInSquareBrackets1")).execute();
    db.command(new OCommandSQL("CREATE CLASS ComplexFilterInSquareBrackets2")).execute();
    db.command(
            new OCommandSQL(
                "INSERT INTO ComplexFilterInSquareBrackets1 SET name = 'n1', value = 1"))
        .execute();
    db.command(
            new OCommandSQL(
                "INSERT INTO ComplexFilterInSquareBrackets1 SET name = 'n2', value = 2"))
        .execute();
    db.command(
            new OCommandSQL(
                "INSERT INTO ComplexFilterInSquareBrackets1 SET name = 'n3', value = 3"))
        .execute();
    db.command(
            new OCommandSQL(
                "INSERT INTO ComplexFilterInSquareBrackets1 SET name = 'n4', value = 4"))
        .execute();
    db.command(
            new OCommandSQL(
                "INSERT INTO ComplexFilterInSquareBrackets1 SET name = 'n5', value = 5"))
        .execute();
    db.command(
            new OCommandSQL(
                "INSERT INTO ComplexFilterInSquareBrackets1 SET name = 'n6', value = -1"))
        .execute();
    db.command(
            new OCommandSQL(
                "INSERT INTO ComplexFilterInSquareBrackets1 SET name = 'n7', value = null"))
        .execute();
    db.command(
            new OCommandSQL(
                "INSERT INTO ComplexFilterInSquareBrackets2 SET collection = (select from ComplexFilterInSquareBrackets1)"))
        .execute();
  }

  private static void initFilterAndOrderByTest(ODatabaseDocumentTx db) {
    db.command(new OCommandSQL("CREATE CLASS FilterAndOrderByTest")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY FilterAndOrderByTest.dc DATETIME")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY FilterAndOrderByTest.active BOOLEAN")).execute();
    db.command(
            new OCommandSQL(
                "CREATE INDEX FilterAndOrderByTest.active ON FilterAndOrderByTest (active) NOTUNIQUE"))
        .execute();

    db.command(
            new OCommandSQL(
                "insert into FilterAndOrderByTest SET dc = '2010-01-05 12:00:00:000', active = true"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into FilterAndOrderByTest SET dc = '2010-05-05 14:00:00:000', active = false"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into FilterAndOrderByTest SET dc = '2009-05-05 16:00:00:000', active = true"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into FilterAndOrderByTest SET dc = '2008-05-05 12:00:00:000', active = false"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into FilterAndOrderByTest SET dc = '2014-05-05 14:00:00:000', active = false"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into FilterAndOrderByTest SET dc = '2016-01-05 14:00:00:000', active = true"))
        .execute();
  }

  private static void initMaxLongNumber(ODatabaseDocumentTx db) {
    db.command(new OCommandSQL("CREATE class MaxLongNumberTest")).execute();
    db.command(new OCommandSQL("insert into MaxLongNumberTest set last = 1")).execute();
    db.command(new OCommandSQL("insert into MaxLongNumberTest set last = null")).execute();
    db.command(new OCommandSQL("insert into MaxLongNumberTest set last = 958769876987698"))
        .execute();
    db.command(new OCommandSQL("insert into MaxLongNumberTest set foo = 'bar'")).execute();
  }

  private static void initLinkListSequence(ODatabaseDocumentTx db) {
    db.command(new OCommandSQL("CREATE class LinkListSequence")).execute();

    db.command(new OCommandSQL("insert into LinkListSequence set name = '1.1.1'")).execute();
    db.command(new OCommandSQL("insert into LinkListSequence set name = '1.1.2'")).execute();
    db.command(new OCommandSQL("insert into LinkListSequence set name = '1.2.1'")).execute();
    db.command(new OCommandSQL("insert into LinkListSequence set name = '1.2.2'")).execute();
    db.command(
            new OCommandSQL(
                "insert into LinkListSequence set name = '1.1', children = (select from LinkListSequence where name like '1.1.%')"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into LinkListSequence set name = '1.2', children = (select from LinkListSequence where name like '1.2.%')"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into LinkListSequence set name = '1', children = (select from LinkListSequence where name in ['1.1', '1.2'])"))
        .execute();
    db.command(new OCommandSQL("insert into LinkListSequence set name = '2'")).execute();
    db.command(
            new OCommandSQL(
                "insert into LinkListSequence set name = 'root', children = (select from LinkListSequence where name in ['1', '1'])"))
        .execute();
  }

  private static void initMatchesWithRegex(ODatabaseInternal<ORecord> db) {
    db.command(new OCommandSQL("CREATE class matchesstuff")).execute();

    db.command(new OCommandSQL("insert into matchesstuff (name, foo) values ('admin[name]', 1)"))
        .execute();
  }

  private static void initDistinctLimit(ODatabaseInternal<ORecord> db) {
    db.command(new OCommandSQL("CREATE class DistinctLimit")).execute();

    db.command(new OCommandSQL("insert into DistinctLimit (name, foo) values ('one', 1)"))
        .execute();
    db.command(new OCommandSQL("insert into DistinctLimit (name, foo) values ('one', 1)"))
        .execute();
    db.command(new OCommandSQL("insert into DistinctLimit (name, foo) values ('two', 2)"))
        .execute();
    db.command(new OCommandSQL("insert into DistinctLimit (name, foo) values ('two', 2)"))
        .execute();
  }

  private static void initDatesSet(ODatabaseDocumentTx db) {
    db.command(new OCommandSQL("create class OCommandExecutorSQLSelectTest_datesSet")).execute();
    db.command(
            new OCommandSQL(
                "create property OCommandExecutorSQLSelectTest_datesSet.foo embeddedlist date"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into OCommandExecutorSQLSelectTest_datesSet set foo = ['2015-10-21']"))
        .execute();
  }

  private static void initMassiveOrderSkipLimit(ODatabaseDocumentTx db) {
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

  private static void initExpandSkipLimit(ODatabaseDocumentTx db) {
    db.command(new OCommandSQL("create class ExpandSkipLimit clusters 1")).execute();

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

  private static OProfiler getProfilerInstance() throws Exception {
    return Orient.instance().getProfiler();
  }

  @AfterClass
  public static void afterClass() throws Exception {
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

    List<ODocument> qResult =
        db.command(
                new OCommandSQL("select * from foo where name ='a' and bar = 1000 or name = 'b'"))
            .execute();

    List<ODocument> qResult2 =
        db.command(
                new OCommandSQL("select * from foo where name = 'b' or name ='a' and bar = 1000"))
            .execute();

    List<ODocument> qResult3 =
        db.command(
                new OCommandSQL("select * from foo where name = 'b' or (name ='a' and bar = 1000)"))
            .execute();

    List<ODocument> qResult4 =
        db.command(
                new OCommandSQL("select * from foo where (name ='a' and bar = 1000) or name = 'b'"))
            .execute();

    List<ODocument> qResult5 =
        db.command(
                new OCommandSQL(
                    "select * from foo where ((name ='a' and bar = 1000) or name = 'b')"))
            .execute();

    List<ODocument> qResult6 =
        db.command(
                new OCommandSQL(
                    "select * from foo where ((name ='a' and (bar = 1000)) or name = 'b')"))
            .execute();

    List<ODocument> qResult7 =
        db.command(
                new OCommandSQL(
                    "select * from foo where (((name ='a' and bar = 1000)) or name = 'b')"))
            .execute();

    List<ODocument> qResult8 =
        db.command(
                new OCommandSQL(
                    "select * from foo where (((name ='a' and bar = 1000)) or (name = 'b'))"))
            .execute();

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
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select * from bar where name ='a' and foo = 1 or name='b' or name='c' and foo = 3 and other = 4 or name = 'e' and foo = 5 or name = 'm' and foo > 2 "))
            .execute();

    List<ODocument> qResult2 =
        db.command(
                new OCommandSQL(
                    "select * from bar where (name ='a' and foo = 1) or name='b' or (name='c' and foo = 3 and other = 4) or (name = 'e' and foo = 5) or (name = 'm' and foo > 2)"))
            .execute();

    List<ODocument> qResult3 =
        db.command(
                new OCommandSQL(
                    "select * from bar where (name ='a' and foo = 1) or (name='b') or (name='c' and foo = 3 and other = 4) or (name ='e' and foo = 5) or (name = 'm' and foo > 2)"))
            .execute();

    List<ODocument> qResult4 =
        db.command(
                new OCommandSQL(
                    "select * from bar where (name ='a' and foo = 1) or ((name='b') or (name='c' and foo = 3 and other = 4)) or (name = 'e' and foo = 5) or (name = 'm' and foo > 2)"))
            .execute();

    List<ODocument> qResult5 =
        db.command(
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
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select * from bar where name <> 'a' and foo = 1 or name='b' or name='c' and foo = 3 and other = 4 or name = 'e' and foo = 5 or name = 'm' and foo > 2 "))
            .execute();

    List<ODocument> qResult2 =
        db.command(
                new OCommandSQL(
                    "select * from bar where (name <> 'a' and foo = 1) or name='b' or (name='c' and foo = 3 and other <>  4) or (name = 'e' and foo = 5) or (name = 'm' and foo > 2)"))
            .execute();

    List<ODocument> qResult3 =
        db.command(
                new OCommandSQL(
                    "select * from bar where ( name <> 'a' and foo = 1) or (name='b') or (name='c' and foo = 3 and other <>  4) or (name ='e' and foo = 5) or (name = 'm' and foo > 2)"))
            .execute();

    List<ODocument> qResult4 =
        db.command(
                new OCommandSQL(
                    "select * from bar where (name <> 'a' and foo = 1) or ( (name='b') or (name='c' and foo = 3 and other <>  4)) or  (name = 'e' and foo = 5) or (name = 'm' and foo > 2)"))
            .execute();

    List<ODocument> qResult5 =
        db.command(
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
    List<ODocument> qResult =
        db.command(new OCommandSQL("select expand(address) from foo where name = 'a'")).execute();

    assertEquals(qResult.size(), 1);
    assertEquals(qResult.get(0).field("city"), "NY");
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
    List<ODocument> qResult = db.command(new OCommandSQL("select from foo limit 3")).execute();
    assertEquals(qResult.size(), 3);
  }

  @Test
  public void testLimitWithMetadataQuery() {
    List<ODocument> qResult =
        db.command(new OCommandSQL("select expand(classes) from metadata:schema limit 3"))
            .execute();
    assertEquals(qResult.size(), 3);
  }

  @Test
  public void testOrderByWithMetadataQuery() {
    List<ODocument> qResult =
        db.command(new OCommandSQL("select expand(classes) from metadata:schema order by name"))
            .execute();
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
    List<ODocument> qResult =
        db.command(new OCommandSQL("select from foo limit :lim")).execute(params);
    assertEquals(qResult.size(), 2);
  }

  @Test
  public void testLimitWithNamedParam2() {
    // issue #5493
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("limit", 2);
    List<ODocument> qResult =
        db.command(new OCommandSQL("select from foo limit :limit")).execute(params);
    assertEquals(qResult.size(), 2);
  }

  @Test
  public void testParamsInLetSubquery() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "foo");
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select from TestParams let $foo = (select name from TestParams where surname = :name) where surname in $foo.name "))
            .execute(params);
    assertEquals(qResult.size(), 1);
  }

  @Test
  public void testBooleanParams() {
    // issue #4224
    List<ODocument> qResult =
        db.command(new OCommandSQL("select name from TestParams where name = ? and active = ?"))
            .execute("foo", true);
    assertEquals(qResult.size(), 1);
  }

  @Test
  public void testFromInSquareBrackets() {
    List<ODocument> qResult =
        db.command(new OCommandSQL("select tags[' from '] as a from TestFromInSquare")).execute();
    assertEquals(qResult.size(), 1);
    assertEquals(qResult.get(0).field("a"), "foo");
  }

  @Test
  public void testNewline() {
    List<ODocument> qResult =
        db.command(new OCommandSQL("select\n1 as ACTIVE\nFROM foo")).execute();
    assertEquals(qResult.size(), 5);
  }

  @Test
  public void testOrderByRid() {
    List<ODocument> qResult =
        db.command(new OCommandSQL("select from ridsorttest order by @rid ASC")).execute();
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

    qResult =
        db.command(new OCommandSQL("select from ridsorttest where name > 3 order by @rid DESC"))
            .execute();
    assertTrue(qResult.size() > 0);

    prev = qResult.get(0);
    for (int i = 1; i < qResult.size(); i++) {
      assertTrue(prev.getIdentity().compareTo(qResult.get(i).getIdentity()) >= 0);
      prev = qResult.get(i);
    }
  }

  @Test
  public void testUnwind() {
    List<ODocument> qResult =
        db.command(new OCommandSQL("select from unwindtest unwind coll")).execute();

    assertEquals(qResult.size(), 4);
    for (ODocument doc : qResult) {
      String name = doc.field("name");
      String coll = doc.field("coll");
      assertTrue(coll.startsWith(name));
      assertFalse(doc.getIdentity().isPersistent());
    }
  }

  @Test
  public void testUnwind2() {
    List<ODocument> qResult =
        db.command(new OCommandSQL("select from unwindtest2 unwind coll")).execute();

    assertEquals(qResult.size(), 1);
    for (ODocument doc : qResult) {
      String name = doc.field("name");
      Object coll = doc.field("coll");
      assertNull(coll);
      assertFalse(doc.getIdentity().isPersistent());
    }
  }

  @Test
  public void testUnwindOrder() {
    List<ODocument> qResult =
        db.command(new OCommandSQL("select from unwindtest order by coll unwind coll")).execute();

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
    List<ODocument> qResult =
        db.command(new OCommandSQL("select from unwindtest unwind coll skip 1")).execute();

    assertEquals(qResult.size(), 3);
    for (ODocument doc : qResult) {
      String name = doc.field("name");
      String coll = doc.field("coll");
      assertTrue(coll.startsWith(name));
    }
  }

  @Test
  public void testUnwindLimit() {
    List<ODocument> qResult =
        db.command(new OCommandSQL("select from unwindtest unwind coll limit 1")).execute();

    assertEquals(qResult.size(), 1);
    for (ODocument doc : qResult) {
      String name = doc.field("name");
      String coll = doc.field("coll");
      assertTrue(coll.startsWith(name));
    }
  }

  @Test
  public void testUnwindLimit3() {
    List<ODocument> qResult =
        db.command(new OCommandSQL("select from unwindtest unwind coll limit 3")).execute();

    assertEquals(qResult.size(), 3);
    for (ODocument doc : qResult) {
      String name = doc.field("name");
      String coll = doc.field("coll");
      assertTrue(coll.startsWith(name));
    }
  }

  @Test
  public void testUnwindSkipAndLimit() {
    List<ODocument> qResult =
        db.command(new OCommandSQL("select from unwindtest unwind coll skip 1 limit 1")).execute();

    assertEquals(qResult.size(), 1);
    for (ODocument doc : qResult) {
      String name = doc.field("name");
      String coll = doc.field("coll");
      assertTrue(coll.startsWith(name));
    }
  }

  @Test
  public void testMultipleClusters() {
    List<ODocument> qResult =
        db.command(new OCommandSQL("select from cluster:[testmultipleclusters1]")).execute();

    assertEquals(qResult.size(), 1);

    qResult =
        db.command(
                new OCommandSQL(
                    "select from cluster:[testmultipleclusters1, testmultipleclusters2]"))
            .execute();

    assertEquals(qResult.size(), 2);
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
    List<ODocument> result =
        db.query(new OSQLSynchQuery<Object>("select *, name as blabla from foo where name = 'a'"));

    assertEquals(result.size(), 1);
    assertEquals(result.get(0).field("blabla"), "a");

    result =
        db.query(new OSQLSynchQuery<Object>("select name as blabla, * from foo where name = 'a'"));

    assertEquals(result.size(), 1);
    assertEquals(result.get(0).field("blabla"), "a");

    result =
        db.query(
            new OSQLSynchQuery<Object>(
                "select name as blabla, *, fff as zzz from foo where name = 'a'"));

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
    List<ODocument> qResult =
        db.command(new OCommandSQL("select from unwindtest unwind coll skip 1 limit 2")).execute();

    assertEquals(qResult.size(), 2);
    for (ODocument doc : qResult) {
      String name = doc.field("name");
      String coll = doc.field("coll");
      assertTrue(coll.startsWith(name));
    }
  }

  @Test
  public void testMultipleParamsWithSameName() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("param1", "foo");
    List<ODocument> qResult =
        db.command(new OCommandSQL("select from TestParams where name like '%' + :param1 + '%'"))
            .execute(params);
    assertEquals(qResult.size(), 2);

    qResult =
        db.command(
                new OCommandSQL(
                    "select from TestParams where name like '%' + :param1 + '%' and surname like '%' + :param1 + '%'"))
            .execute(params);
    assertEquals(qResult.size(), 1);

    params = new HashMap<String, Object>();
    params.put("param1", "bar");

    qResult =
        db.command(new OCommandSQL("select from TestParams where surname like '%' + :param1 + '%'"))
            .execute(params);
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
    OSQLSynchQuery sql =
        new OSQLSynchQuery(
            "SELECT $ll as lll from unwindtest let $ll = coll.asList().asString() where name = 'bar'");
    List<ODocument> results = db.query(sql);
    assertEquals(1, results.size());
    assertNotNull(results.get(0).field("lll"));
    assertEquals("[bar1, bar2]", results.get(0).field("lll"));
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
    OSQLSynchQuery sql =
        new OSQLSynchQuery(
            "SELECT 1 AS integer, 'Test' AS string, NULL AS nothing, [] AS array, {} AS object");

    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 1);
    ODocument doc = results.get(0);
    assertThat(doc.<Integer>field("integer")).isEqualTo(1);
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
    OSQLSynchQuery sql =
        new OSQLSynchQuery(
            "SELECT expand(linked) from ExpandSkipLimit where parent = true order by nnum skip 1 limit 1");
    List<OIdentifiable> results = db.query(sql);
    assertEquals(results.size(), 1);
    ODocument doc = results.get(0).getRecord();
    //    assertEquals(doc.field("nnum"), 1);
    assertThat(doc.<Integer>field("nnum")).isEqualTo(1);
  }

  @Test
  public void testBacktick() {
    OSQLSynchQuery sql = new OSQLSynchQuery("SELECT `foo-bar` as r from TestBacktick");
    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 1);
    ODocument doc = results.get(0);
    //    assertEquals(doc.field("r"), 10);
    assertThat(doc.<Integer>field("r")).isEqualTo(10);
  }

  @Test
  public void testOrderByEmbeddedParams() {
    // issue #4949
    Map<String, Object> parameters = new HashMap<String, Object>();
    parameters.put("paramvalue", "count");
    List<ODocument> qResult =
        db.command(new OCommandSQL("select from TestParamsEmbedded order by emb[:paramvalue] DESC"))
            .execute(parameters);
    assertEquals(qResult.size(), 2);
    Map embedded = qResult.get(0).field("emb");
    assertEquals(embedded.get("count"), 1);
  }

  @Test
  public void testOrderByEmbeddedParams2() {
    // issue #4949
    Map<String, Object> parameters = new HashMap<String, Object>();
    parameters.put("paramvalue", "count");
    List<ODocument> qResult =
        db.command(new OCommandSQL("select from TestParamsEmbedded order by emb[:paramvalue] ASC"))
            .execute(parameters);
    assertEquals(qResult.size(), 2);
    Map embedded = qResult.get(0).field("emb");
    assertEquals(embedded.get("count"), 0);
  }

  @Test
  public void testMassiveOrderAscSkipLimit() {
    int skip = 1000;
    OSQLSynchQuery sql =
        new OSQLSynchQuery(
            "SELECT from MassiveOrderSkipLimit order by nnum asc skip " + skip + " limit 5");
    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 5);
    for (int i = 0; i < results.size(); i++) {
      ODocument doc = results.get(i);
      //      assertEquals(doc.field("nnum"), skip + i);
      assertThat(doc.<Integer>field("nnum")).isEqualTo(skip + i);
    }
  }

  @Test
  public void testMassiveOrderDescSkipLimit() {
    int skip = 1000;
    OSQLSynchQuery sql =
        new OSQLSynchQuery(
            "SELECT from MassiveOrderSkipLimit order by nnum desc skip " + skip + " limit 5");

    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 5);
    for (int i = 0; i < results.size(); i++) {
      ODocument doc = results.get(i);
      //      assertEquals(doc.field("nnum"), ORDER_SKIP_LIMIT_ITEMS - 1 - skip - i);
      assertThat(doc.<Integer>field("nnum")).isEqualTo(ORDER_SKIP_LIMIT_ITEMS - 1 - skip - i);
    }
  }

  @Test
  public void testIntersectExpandLet() {
    // issue #5121
    OSQLSynchQuery sql =
        new OSQLSynchQuery(
            "select expand(intersect($q1, $q2)) "
                + "let $q1 = (select from OUser where name ='admin'),"
                + "$q2 = (select from OUser where name ='admin')");

    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 1);
    for (int i = 0; i < results.size(); i++) {
      ODocument doc = results.get(i);
      assertEquals(doc.field("name"), "admin");
    }
  }

  @Test
  public void testDatesListContainsString() {
    // issue #3526
    OSQLSynchQuery sql =
        new OSQLSynchQuery(
            "select from OCommandExecutorSQLSelectTest_datesSet where foo contains '2015-10-21'");

    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 1);
  }

  @Test
  public void testParamWithMatches() {
    // issue #5229
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("param1", "adm.*");
    OSQLSynchQuery sql = new OSQLSynchQuery("select from OUser where name matches :param1");
    List<ODocument> results = db.query(sql, params);
    assertEquals(results.size(), 1);
  }

  @Test
  public void testParamWithMatchesQuoteRegex() {
    // issue #5229
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("param1", ".*admin[name].*"); // will not work
    OSQLSynchQuery sql = new OSQLSynchQuery("select from matchesstuff where name matches :param1");
    List<ODocument> results = db.query(sql, params);
    assertEquals(results.size(), 0);
    params.put("param1", Pattern.quote("admin[name]") + ".*"); // should work
    results = db.query(sql, params);
    assertEquals(results.size(), 1);
  }

  @Test
  public void testMatchesWithQuotes() {
    // issue #5229
    String pattern = Pattern.quote("adm") + ".*";
    OSQLSynchQuery sql = new OSQLSynchQuery("SELECT FROM matchesstuff WHERE (name matches ?)");
    List<ODocument> results = db.query(sql, pattern);
    assertEquals(results.size(), 1);
  }

  @Test
  public void testMatchesWithQuotes2() {
    // issue #5229
    OSQLSynchQuery sql =
        new OSQLSynchQuery(
            "SELECT FROM matchesstuff WHERE (name matches '\\\\Qadm\\\\E.*' and not ( name matches '(.*)foo(.*)' ) )");
    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 1);
  }

  @Test
  public void testMatchesWithQuotes3() {
    // issue #5229
    OSQLSynchQuery sql =
        new OSQLSynchQuery(
            "SELECT FROM matchesstuff WHERE (name matches '\\\\Qadm\\\\E.*' and  ( name matches '\\\\Qadmin\\\\E.*' ) )");
    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 1);
  }

  @Test
  public void testParamWithMatchesAndNot() {
    // issue #5229
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("param1", "adm.*");
    params.put("param2", "foo.*");
    OSQLSynchQuery sql =
        new OSQLSynchQuery(
            "select from OUser where (name matches :param1 and not (name matches :param2))");
    List<ODocument> results = db.query(sql, params);
    assertEquals(results.size(), 1);

    params.put("param1", Pattern.quote("adm") + ".*");
    results = db.query(sql, params);
    assertEquals(results.size(), 1);
  }

  @Test
  public void testDistinctLimit() {
    OSQLSynchQuery sql = new OSQLSynchQuery("select distinct(name) from DistinctLimit limit 1");
    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 1);

    sql = new OSQLSynchQuery("select distinct(name) from DistinctLimit limit 2");
    results = db.query(sql);
    assertEquals(results.size(), 2);

    sql = new OSQLSynchQuery("select distinct(name) from DistinctLimit limit 3");
    results = db.query(sql);
    assertEquals(results.size(), 2);

    sql = new OSQLSynchQuery("select distinct(name) from DistinctLimit limit -1");
    results = db.query(sql);
    assertEquals(results.size(), 2);
  }

  @Test
  public void testSelectFromClusterNumber() {
    OClass clazz = db.getMetadata().getSchema().getClass("DistinctLimit");
    int clusterId = clazz.getClusterIds()[0];
    OSQLSynchQuery sql = new OSQLSynchQuery("select from cluster:" + clusterId + " limit 1");
    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 1);
  }

  @Test
  public void testLinkListSequence1() {
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
    OSQLSynchQuery sql =
        new OSQLSynchQuery(
            "select expand(children[0].children[0].children) from LinkListSequence where name = 'root'");
    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 2);
    for (ODocument result : results) {
      String value = result.field("name");
      assertTrue(value.equals("1.1.1") || value.equals("1.1.2"));
    }
  }

  @Test
  public void testMaxLongNumber() {
    // issue #5664
    OSQLSynchQuery sql =
        new OSQLSynchQuery("select from MaxLongNumberTest WHERE last < 10 OR last is null");
    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 3);
    db.command(new OCommandSQL("update MaxLongNumberTest set last = max(91,ifnull(last,0))"))
        .execute();
    sql = new OSQLSynchQuery("select from MaxLongNumberTest WHERE last < 10 OR last is null");
    results = db.query(sql);
    assertEquals(results.size(), 0);
  }

  @Test
  public void testFilterAndOrderBy() {
    // issue http://www.prjhub.com/#/issues/6199

    OSQLSynchQuery sql =
        new OSQLSynchQuery("SELECT FROM FilterAndOrderByTest WHERE active = true ORDER BY dc DESC");
    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 3);

    Calendar cal = new GregorianCalendar();

    Date date = results.get(0).field("dc");
    cal.setTime(date);
    assertEquals(cal.get(Calendar.YEAR), 2016);

    date = results.get(1).field("dc");
    cal.setTime(date);
    assertEquals(cal.get(Calendar.YEAR), 2010);

    date = results.get(2).field("dc");
    cal.setTime(date);
    assertEquals(cal.get(Calendar.YEAR), 2009);
  }

  @Test
  public void testComplexFilterInSquareBrackets() {
    // issues #513 #5451

    OSQLSynchQuery sql =
        new OSQLSynchQuery(
            "SELECT expand(collection[name = 'n1']) FROM ComplexFilterInSquareBrackets2");
    List<ODocument> results = db.query(sql);
    assertEquals(results.size(), 1);
    assertEquals(results.iterator().next().field("name"), "n1");

    sql =
        new OSQLSynchQuery(
            "SELECT expand(collection[name = 'n1' and value = 1]) FROM ComplexFilterInSquareBrackets2");
    results = db.query(sql);
    assertEquals(results.size(), 1);
    assertEquals(results.iterator().next().field("name"), "n1");

    sql =
        new OSQLSynchQuery(
            "SELECT expand(collection[name = 'n1' and value > 1]) FROM ComplexFilterInSquareBrackets2");
    results = db.query(sql);
    assertEquals(results.size(), 0);

    sql =
        new OSQLSynchQuery(
            "SELECT expand(collection[name = 'n1' or value = -1]) FROM ComplexFilterInSquareBrackets2");
    results = db.query(sql);
    assertEquals(results.size(), 2);
    for (ODocument doc : results) {
      assertTrue(doc.field("name").equals("n1") || doc.field("value").equals(-1));
    }

    sql =
        new OSQLSynchQuery(
            "SELECT expand(collection[name = 'n1' and not value = 1]) FROM ComplexFilterInSquareBrackets2");
    results = db.query(sql);
    assertEquals(results.size(), 0);

    sql =
        new OSQLSynchQuery(
            "SELECT expand(collection[value < 0]) FROM ComplexFilterInSquareBrackets2");
    results = db.query(sql);
    assertEquals(results.size(), 1);
    //    assertEquals(results.iterator().next().field("value"), -1);
    assertThat(results.iterator().next().<Integer>field("value")).isEqualTo(-1);

    sql = new OSQLSynchQuery("SELECT expand(collection[2]) FROM ComplexFilterInSquareBrackets2");
    results = db.query(sql);
    assertEquals(results.size(), 1);
  }

  @Test
  public void testCollateOnCollections() {
    // issue #4851
    db.command(new OCommandSQL("create class OCommandExecutorSqlSelectTest_collateOnCollections"))
        .execute();
    db.command(
            new OCommandSQL(
                "create property OCommandExecutorSqlSelectTest_collateOnCollections.categories EMBEDDEDLIST string"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into OCommandExecutorSqlSelectTest_collateOnCollections set categories=['a','b']"))
        .execute();
    db.command(
            new OCommandSQL(
                "alter property OCommandExecutorSqlSelectTest_collateOnCollections.categories COLLATE ci"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into OCommandExecutorSqlSelectTest_collateOnCollections set categories=['Math','English']"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into OCommandExecutorSqlSelectTest_collateOnCollections set categories=['a','b','c']"))
        .execute();
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
    db.command(new OCommandSQL("create class OCommandExecutorSqlSelectTest_testCountUniqueIndex"))
        .execute();
    db.command(
            new OCommandSQL(
                "create property OCommandExecutorSqlSelectTest_testCountUniqueIndex.AAA String"))
        .execute();
    db.command(
            new OCommandSQL(
                "create index OCommandExecutorSqlSelectTest_testCountUniqueIndex.AAA unique"))
        .execute();

    List<ODocument> results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select count(*) from OCommandExecutorSqlSelectTest_testCountUniqueIndex where AAA='missing'"));
    assertEquals(results.size(), 1);
    //    assertEquals(results.iterator().next().field("count"), 0l);

    assertThat(results.iterator().next().<Long>field("count")).isEqualTo(0l);
  }

  @Test
  public void testEvalLong() {
    // http://www.prjhub.com/#/issues/6472
    List<ODocument> results =
        db.query(new OSQLSynchQuery<ODocument>("SELECT EVAL(\"86400000 * 26\") AS value"));
    assertEquals(results.size(), 1);

    //    assertEquals(results.get(0).field("value"), 86400000l * 26);
    assertThat(results.get(0).<Long>field("value")).isEqualTo(86400000l * 26);
  }

  @Test
  public void testCollateOnLinked() {
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
    List<ODocument> results =
        db.query(
            new OSQLSynchQuery<ODocument>("select from TestParams where surname like ? + '%'"),
            "fo");
    assertEquals(results.size(), 1);
  }

  @Test
  public void testCompositeIndexWithoutNullValues() {
    db.command(new OCommandSQL("create class CompositeIndexWithoutNullValues")).execute();
    db.command(new OCommandSQL("create property CompositeIndexWithoutNullValues.one String"))
        .execute();
    db.command(new OCommandSQL("create property CompositeIndexWithoutNullValues.two String"))
        .execute();
    db.command(
            new OCommandSQL(
                "create index CompositeIndexWithoutNullValues.one_two on CompositeIndexWithoutNullValues (one, two) NOTUNIQUE METADATA {ignoreNullValues: true}"))
        .execute();

    db.command(new OCommandSQL("insert into CompositeIndexWithoutNullValues set one = 'foo'"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into CompositeIndexWithoutNullValues set one = 'foo', two = 'bar'"))
        .execute();
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

    db.command(new OCommandSQL("create class CompositeIndexWithoutNullValues2")).execute();
    db.command(new OCommandSQL("create property CompositeIndexWithoutNullValues2.one String"))
        .execute();
    db.command(new OCommandSQL("create property CompositeIndexWithoutNullValues2.two String"))
        .execute();
    db.command(
            new OCommandSQL(
                "create index CompositeIndexWithoutNullValues2.one_two on CompositeIndexWithoutNullValues2 (one, two) NOTUNIQUE METADATA {ignoreNullValues: false}"))
        .execute();

    db.command(new OCommandSQL("insert into CompositeIndexWithoutNullValues2 set one = 'foo'"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into CompositeIndexWithoutNullValues2 set one = 'foo', two = 'bar'"))
        .execute();
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
    List<ODocument> results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select date('2015-07-20', 'yyyy-MM-dd').format('dd.MM.yyyy') as dd"));
    assertEquals(results.size(), 1);
    assertEquals(results.get(0).field("dd"), "20.07.2015");
  }

  @Test
  public void testConcatenateNamedParams() {
    // issue #5572
    List<ODocument> results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select from TestMultipleClusters where name like :p1 + '%'"),
            "fo");
    assertEquals(results.size(), 1);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>("select from TestMultipleClusters where name like :p1 "),
            "fo");
    assertEquals(results.size(), 0);
  }

  @Test
  public void testMethodsOnStrings() {
    // issue #5671
    List<ODocument> results =
        db.query(new OSQLSynchQuery<ODocument>("select '1'.asLong() as long"));
    assertEquals(results.size(), 1);
    //    assertEquals(results.get(0).field("long"), 1L);
    assertThat(results.get(0).<Long>field("long")).isEqualTo(1L);
  }

  @Test
  public void testDifferenceOfInlineCollections() {
    // issue #5294
    List<ODocument> results =
        db.query(new OSQLSynchQuery<ODocument>("select difference([1,2,3],[1,2]) as difference"));
    assertEquals(results.size(), 1);
    Object differenceFieldValue = results.get(0).field("difference");
    assertTrue(differenceFieldValue instanceof Collection);
    assertEquals(((Collection) differenceFieldValue).size(), 1);
    assertEquals(((Collection) differenceFieldValue).iterator().next(), 3);
  }

  @Test
  public void testFoo() {
    // dispose it!
    db.command(new OCommandSQL("create class testFoo")).execute();
    db.command(new OCommandSQL("insert into testFoo set val = 1, name = 'foo'")).execute();
    db.command(new OCommandSQL("insert into testFoo set val = 3, name = 'foo'")).execute();
    db.command(new OCommandSQL("insert into testFoo set val = 5, name = 'bar'")).execute();

    List<ODocument> results =
        db.query(new OSQLSynchQuery<ODocument>("select sum(val), name from testFoo group by name"));
    assertEquals(results.size(), 2);
  }

  @Test
  public void testDateComparison() {
    // issue #6389

    byte[] array = new byte[] {1, 4, 5, 74, 3, 45, 6, 127, -120, 2};

    db.command(new OCommandSQL("create class TestDateComparison")).execute();
    db.command(new OCommandSQL("create property TestDateComparison.dateProp DATE")).execute();

    db.command(new OCommandSQL("insert into TestDateComparison set dateProp = '2016-05-01'"))
        .execute();

    List<ODocument> results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "SELECT from TestDateComparison WHERE dateProp >= '2016-05-01'"));
    assertEquals(results.size(), 1);
    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "SELECT from TestDateComparison WHERE dateProp <= '2016-05-01'"));
    assertEquals(results.size(), 1);
  }
  // <<<<<<< HEAD
  // =======

  @Test
  public void testOrderByRidDescMultiCluster() {
    // issue #6694

    OClass clazz = db.getMetadata().getSchema().createClass("TestOrderByRidDescMultiCluster");
    if (clazz.getClusterIds().length < 2) {
      clazz.addCluster("TestOrderByRidDescMultiCluster_11111");
    }
    for (int i = 0; i < 100; i++) {
      db.command(new OCommandSQL("insert into TestOrderByRidDescMultiCluster set foo = " + i))
          .execute();
    }

    List<ODocument> results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "SELECT from TestOrderByRidDescMultiCluster order by @rid desc"));
    assertEquals(results.size(), 100);
    ODocument lastDoc = null;
    for (ODocument doc : results) {
      if (lastDoc != null) {
        assertTrue(doc.getIdentity().compareTo(lastDoc.getIdentity()) < 0);
      }
      lastDoc = doc;
    }

    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "SELECT from TestOrderByRidDescMultiCluster order by @rid asc"));
    assertEquals(results.size(), 100);
    lastDoc = null;
    for (ODocument doc : results) {
      if (lastDoc != null) {
        assertTrue(doc.getIdentity().compareTo(lastDoc.getIdentity()) > 0);
      }
      lastDoc = doc;
    }
  }

  @Test
  public void testCountOnSubclassIndexes() {
    // issue #6737

    db.command(new OCommandSQL("create class testCountOnSubclassIndexes_superclass")).execute();
    db.command(new OCommandSQL("create property testCountOnSubclassIndexes_superclass.foo boolean"))
        .execute();
    db.command(
            new OCommandSQL(
                "create index testCountOnSubclassIndexes_superclass.foo on testCountOnSubclassIndexes_superclass (foo) notunique"))
        .execute();

    db.command(
            new OCommandSQL(
                "create class testCountOnSubclassIndexes_sub1 extends testCountOnSubclassIndexes_superclass"))
        .execute();
    db.command(
            new OCommandSQL(
                "create index testCountOnSubclassIndexes_sub1.foo on testCountOnSubclassIndexes_sub1 (foo) notunique"))
        .execute();

    db.command(
            new OCommandSQL(
                "create class testCountOnSubclassIndexes_sub2 extends testCountOnSubclassIndexes_superclass"))
        .execute();
    db.command(
            new OCommandSQL(
                "create index testCountOnSubclassIndexes_sub2.foo on testCountOnSubclassIndexes_sub2 (foo) notunique"))
        .execute();

    db.command(
            new OCommandSQL(
                "insert into testCountOnSubclassIndexes_sub1 set name = 'a', foo = true"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into testCountOnSubclassIndexes_sub1 set name = 'b', foo = false"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into testCountOnSubclassIndexes_sub2 set name = 'c', foo = true"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into testCountOnSubclassIndexes_sub2 set name = 'd', foo = true"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into testCountOnSubclassIndexes_sub2 set name = 'e', foo = false"))
        .execute();

    List<ODocument> results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "SELECT count(*) from testCountOnSubclassIndexes_sub1 where foo = true"));
    assertEquals(results.size(), 1);
    assertEquals(results.get(0).field("count"), (Object) 1L);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "SELECT count(*) from testCountOnSubclassIndexes_sub2 where foo = true"));
    assertEquals(results.size(), 1);
    assertEquals(results.get(0).field("count"), (Object) 2L);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "SELECT count(*) from testCountOnSubclassIndexes_superclass where foo = true"));
    assertEquals(results.size(), 1);
    assertEquals(results.get(0).field("count"), (Object) 3L);
  }

  @Test
  public void testDoubleExponentNotation() {
    // issue #7013

    List<ODocument> results = db.query(new OSQLSynchQuery<ODocument>("select 1e-2 as a"));
    assertEquals(results.size(), 1);
    assertEquals(results.get(0).field("a"), (Object) 0.01d);
  }

  @Test
  public void testConvertDouble() {
    // issue #7234

    db.command(new OCommandSQL("create class testConvertDouble")).execute();
    db.command(new OCommandSQL("insert into testConvertDouble set num = 100000")).execute();

    List<ODocument> results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "SELECT FROM testConvertDouble WHERE num >= 50000 AND num <=300000000"));
    assertEquals(results.size(), 1);
  }

  @Test
  public void testFilterListsOfMaps() {

    String className = "testFilterListaOfMaps";

    db.command(new OCommandSQL("create class " + className)).execute();
    db.command(new OCommandSQL("create property " + className + ".tagz embeddedmap")).execute();
    db.command(new OCommandSQL("insert into " + className + " set tagz = {}")).execute();
    db.command(
            new OCommandSQL(
                "update "
                    + className
                    + " SET tagz.foo = [{name:'a', surname:'b'}, {name:'c', surname:'d'}]"))
        .execute();

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
    db.command(new OCommandSQL("create class " + className)).execute();
    db.command(new OCommandSQL("create property " + className + ".state Short")).execute();
    db.command(new OCommandSQL("INSERT INTO " + className + " set state = 1")).execute();
    db.command(new OCommandSQL("INSERT INTO " + className + " set state = 1")).execute();
    db.command(new OCommandSQL("INSERT INTO " + className + " set state = 2")).execute();

    List<ODocument> results =
        db.query(new OSQLSynchQuery<ODocument>("select from " + className + " where state in [1]"));
    assertEquals(results.size(), 2);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select from " + className + " where [1] contains state"));
    assertEquals(results.size(), 2);
  }

  @Test
  public void testEnumAsParams() {
    // issue #7418
    String className = "testEnumAsParams";
    db.command(new OCommandSQL("create class " + className)).execute();
    db.command(new OCommandSQL("INSERT INTO " + className + " set status = ?"))
        .execute(OType.STRING);
    db.command(new OCommandSQL("INSERT INTO " + className + " set status = ?")).execute(OType.ANY);
    db.command(new OCommandSQL("INSERT INTO " + className + " set status = ?")).execute(OType.BYTE);

    Map<String, Object> params = new HashMap<String, Object>();
    List enums = new ArrayList();
    enums.add(OType.STRING);
    enums.add(OType.BYTE);
    params.put("status", enums);
    List<ODocument> results =
        db.query(
            new OSQLSynchQuery<ODocument>("select from " + className + " where status in :status"),
            params);
    assertEquals(results.size(), 2);
  }

  @Test
  public void testEmbeddedMapOfMapsContainsValue() {
    // issue #7793
    String className = "testEmbeddedMapOfMapsContainsValue";

    db.command(new OCommandSQL("create class " + className)).execute();
    db.command(new OCommandSQL("create property " + className + ".embedded_map EMBEDDEDMAP"))
        .execute();
    db.command(new OCommandSQL("create property " + className + ".id INTEGER")).execute();
    db.command(
            new OCommandSQL(
                "INSERT INTO "
                    + className
                    + " SET id = 0, embedded_map = {\"key_2\" : {\"name\" : \"key_2\", \"id\" : \"0\"}}"))
        .execute();
    db.command(
            new OCommandSQL(
                "INSERT INTO "
                    + className
                    + " SET id = 1, embedded_map = {\"key_1\" : {\"name\" : \"key_1\", \"id\" : \"1\" }}"))
        .execute();

    List<ODocument> results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select from "
                    + className
                    + " where embedded_map CONTAINSVALUE {\"name\":\"key_2\", \"id\":\"0\"}"));
    assertEquals(results.size(), 1);
  }

  @Test
  public void testInvertedIndexedCondition() {
    // issue #7820
    String className = "testInvertedIndexedCondition";

    db.command(new OCommandSQL("create class " + className)).execute();
    db.command(new OCommandSQL("create property " + className + ".name STRING")).execute();
    db.command(new OCommandSQL("insert into " + className + " SET name = \"1\"")).execute();
    db.command(new OCommandSQL("insert into " + className + " SET name = \"2\"")).execute();

    List<ODocument> results =
        db.query(
            new OSQLSynchQuery<ODocument>("SELECT * FROM " + className + " WHERE name >= \"0\""));
    assertEquals(results.size(), 2);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>("SELECT * FROM " + className + " WHERE \"0\" <= name"));
    assertEquals(results.size(), 2);

    db.command(
            new OCommandSQL(
                "CREATE INDEX " + className + ".name on " + className + " (name) UNIQUE"))
        .execute();

    results =
        db.query(
            new OSQLSynchQuery<ODocument>("SELECT * FROM " + className + " WHERE \"0\" <= name"));
    assertEquals(results.size(), 2);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>("SELECT * FROM " + className + " WHERE \"2\" <= name"));
    assertEquals(results.size(), 1);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>("SELECT * FROM " + className + " WHERE name >= \"0\""));
    assertEquals(results.size(), 2);
  }

  @Test
  public void testIsDefinedOnNull() {
    // issue #7879
    String className = "testIsDefinedOnNull";

    db.command(new OCommandSQL("create class " + className)).execute();
    db.command(new OCommandSQL("create property " + className + ".name STRING")).execute();
    db.command(new OCommandSQL("insert into " + className + " SET name = null, x = 1")).execute();
    db.command(new OCommandSQL("insert into " + className + " SET x = 2")).execute();

    List<ODocument> results =
        db.query(
            new OSQLSynchQuery<ODocument>("SELECT * FROM " + className + " WHERE name is defined"));
    assertEquals(results.size(), 1);
    assertEquals((int) results.get(0).field("x"), 1);

    results =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "SELECT * FROM " + className + " WHERE name is not defined"));
    assertEquals(results.size(), 1);
    assertEquals((int) results.get(0).field("x"), 2);
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
}
