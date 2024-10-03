/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "sql-select")
public class SQLSelectProjectionsTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SQLSelectProjectionsTest(@Optional String url) {
    super(url);
  }

  @Test
  public void queryProjectionOk() {
    List<OResult> result =
        database
            .command(
                " select nick, followings, followers from Profile where nick is defined and"
                    + " followings is defined and followers is defined")
            .stream()
            .toList();

    Assert.assertTrue(result.size() != 0);

    for (OResult d : result) {
      String[] colNames = d.getPropertyNames().toArray(new String[] {});
      Assert.assertEquals(colNames.length, 3, "document: " + d);
      Assert.assertEquals(colNames[0], "nick", "document: " + d);
      Assert.assertEquals(colNames[1], "followings", "document: " + d);
      Assert.assertEquals(colNames[2], "followers", "document: " + d);

      Assert.assertFalse(d.getElement().isPresent());
    }
  }

  @Test
  public void queryProjectionObjectLevel() {
    ODatabaseObject db = new OObjectDatabaseTx(url);
    db.open("admin", "admin");

    List<OResult> result =
        db.getUnderlying().query(" select nick, followings, followers from Profile ").stream()
            .toList();

    Assert.assertTrue(result.size() != 0);

    for (OResult d : result) {
      Assert.assertTrue(d.getPropertyNames().size() <= 3);
      Assert.assertFalse(d.getElement().isPresent());
    }

    db.close();
  }

  @Test
  public void queryProjectionLinkedAndFunction() {
    List<OResult> result =
        database
            .command(
                "select name.toUpperCase(Locale.ENGLISH), address.city.country.name from"
                    + " Profile")
            .stream()
            .toList();

    Assert.assertTrue(result.size() != 0);

    for (OResult d : result) {
      Assert.assertTrue(d.getPropertyNames().size() <= 2);
      if (d.getProperty("name") != null)
        Assert.assertTrue(
            d.getProperty("name")
                .equals(((String) d.getProperty("name")).toUpperCase(Locale.ENGLISH)));

      Assert.assertFalse(d.getElement().isPresent());
    }
  }

  @Test
  public void queryProjectionSameFieldTwice() {
    List<OResult> result =
        database
            .command(
                "select name, name.toUpperCase(Locale.ENGLISH) as name2 from Profile where name is"
                    + " not null")
            .stream()
            .toList();

    Assert.assertTrue(result.size() != 0);

    for (OResult d : result) {
      Assert.assertTrue(d.getPropertyNames().size() <= 2);
      Assert.assertNotNull(d.getProperty("name"));
      Assert.assertNotNull(d.getProperty("name2"));

      Assert.assertFalse(d.getElement().isPresent());
    }
  }

  @Test
  public void queryProjectionStaticValues() {
    List<OResult> result =
        database
            .command(
                "select location.city.country.name as location, address.city.country.name from"
                    + " Profile where location.city.country.name is not null")
            .stream()
            .toList();

    Assert.assertTrue(result.size() != 0);

    for (OResult d : result) {

      Assert.assertNotNull(d.getProperty("location"));
      Assert.assertNull(d.getProperty("address"));

      Assert.assertFalse(d.getElement().isPresent());
    }
  }

  @Test
  public void queryProjectionPrefixAndAppend() {
    List<OResult> result =
        database
            .command(
                "select *, name.prefix('Mr. ').append(' ').append(surname).append('!') as test"
                    + " from Profile where name is not null")
            .stream()
            .toList();

    Assert.assertTrue(result.size() != 0);

    for (OResult d : result) {
      Assert.assertEquals(
          d.getProperty("test").toString(),
          "Mr. " + d.getProperty("name") + " " + d.getProperty("surname") + "!");
    }
  }

  @Test
  public void queryProjectionFunctionsAndFieldOperators() {
    List<OResult> result =
        database
            .command(
                "select name.append('.').prefix('Mr. ') as name from Profile where name is not"
                    + " null")
            .stream()
            .toList();

    Assert.assertTrue(result.size() != 0);

    for (OResult d : result) {
      Assert.assertTrue(d.getPropertyNames().size() <= 1);
      Assert.assertTrue(d.getProperty("name").toString().startsWith("Mr. "));
      Assert.assertTrue(d.getProperty("name").toString().endsWith("."));

      Assert.assertFalse(d.getElement().isPresent());
    }
  }

  // TODO invalid test, invalid integer alias
  // @Test
  // public void queryProjectionAliases() {
  // List<ODocument> result = database.command(
  // new OSQLSynchQuery<ODocument>(
  // "select name.append('!') as 1, surname as 2 from Profile where name is not null and surname is
  // not null")).execute();
  //
  // Assert.assertTrue(result.size() != 0);
  //
  // for (ODocument d : result) {
  // Assert.assertTrue(d.fieldNames().length <= 2);
  // Assert.assertTrue(d.field("1").toString().endsWith("!"));
  // Assert.assertNotNull(d.field("2"));
  //
  // Assert.assertNull(d.getClassName());
  // Assert.assertEquals(ORecordInternal.getRecordType(d), ODocument.RECORD_TYPE);
  // }
  // }

  @Test
  public void queryProjectionSimpleValues() {
    List<OResult> result =
        database.command("select 10, 'ciao' from Profile LIMIT 1").stream().toList();

    Assert.assertTrue(result.size() != 0);

    for (OResult d : result) {
      Assert.assertTrue(d.getPropertyNames().size() <= 2);
      Assert.assertEquals(((Integer) d.getProperty("10")).intValue(), 10l);
      Assert.assertEquals(d.getProperty("'ciao'"), "ciao");

      Assert.assertFalse(d.getElement().isPresent());
    }
  }

  @Test
  public void queryProjectionJSON() {
    List<OResult> result =
        database.command("select @this.toJson() as json from Profile").stream().toList();

    Assert.assertTrue(result.size() != 0);

    for (OResult d : result) {
      Assert.assertTrue(d.getPropertyNames().size() <= 1);
      Assert.assertNotNull(d.getProperty("json"));

      new ODocument().fromJSON((String) d.getProperty("json"));
    }
  }

  public void queryProjectionRid() {
    List<OResult> result = database.command("select @rid as rid FROM V").stream().toList();
    Assert.assertTrue(result.size() != 0);

    for (OResult d : result) {
      Assert.assertTrue(d.getPropertyNames().size() <= 1);
      Assert.assertNotNull(d.getProperty("rid"));

      final ORID rid = d.getProperty("rid");
      Assert.assertTrue(rid.isValid());
    }
  }

  public void queryProjectionOrigin() {
    List<ODocument> result =
        database.command(new OSQLSynchQuery<ODocument>("select @raw FROM V")).execute();
    Assert.assertTrue(result.size() != 0);

    for (ODocument d : result) {
      Assert.assertTrue(d.fieldNames().length <= 1);
      Assert.assertNotNull(d.field("raw"));
    }
  }

  public void queryProjectionEval() {
    List<OResult> result = database.command("select eval('1 + 4') as result").stream().toList();
    Assert.assertEquals(result.size(), 1);

    for (OResult d : result) Assert.assertEquals(d.<Object>getProperty("result"), 5);
  }

  public void queryProjectionContextArray() {
    List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select $a[0] as a0, $a as a from V let $a = outE() where outE().size() > 0"))
            .execute();
    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("a"));
      Assert.assertTrue(d.containsField("a0"));

      final ODocument a0doc = d.field("a0");
      final ODocument firstADoc =
          (ODocument) d.<Iterable<OIdentifiable>>field("a").iterator().next();

      Assert.assertTrue(
          ODocumentHelper.hasSameContentOf(a0doc, database, firstADoc, database, null));
    }
  }

  public void ifNullFunction() {
    List<OResult> result = database.command("SELECT ifnull('a', 'b') as ifnull").stream().toList();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.get(0).getProperty("ifnull"), "a");

    result = database.command("SELECT ifnull('a', 'b', 'c') as ifnull ").stream().toList();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.get(0).getProperty("ifnull"), "c");

    result = database.command("SELECT ifnull(null, 'b') as ifnull").stream().toList();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.get(0).getProperty("ifnull"), "b");
  }

  public void filteringArrayInChain() {
    List<OResult> result =
        database.command("SELECT set(name)[0..1] as set from OUser").stream().toList();
    Assert.assertEquals(result.size(), 1);
    for (OResult d : result) {
      Assert.assertTrue(OMultiValue.isMultiValue(d.<Object>getProperty("set")));
      Assert.assertTrue(OMultiValue.getSize(d.getProperty("set")) <= 2);
    }

    result = database.command("SELECT set(name)[0,1] as set from OUser").stream().toList();
    Assert.assertEquals(result.size(), 1);
    for (OResult d : result) {
      Assert.assertTrue(OMultiValue.isMultiValue(d.<Object>getProperty("set")));
      Assert.assertTrue(OMultiValue.getSize(d.getProperty("set")) <= 2);
    }

    result = database.command("SELECT set(name)[0] as unique from OUser").stream().toList();
    Assert.assertEquals(result.size(), 1);
    for (OResult d : result) {
      Assert.assertFalse(OMultiValue.isMultiValue(d.<Object>getProperty("unique")));
    }
  }

  public void projectionWithNoTarget() {
    List<OResult> result = database.command("select 'Ay' as a , 'bEE'").stream().toList();
    Assert.assertEquals(result.size(), 1);
    for (OResult d : result) {
      Assert.assertTrue(d.getProperty("a").equals("Ay"));
      Assert.assertTrue(d.getProperty("'bEE'").equals("bEE"));
    }

    result = database.command("select 'Ay' as a , 'bEE' as b").stream().toList();
    Assert.assertEquals(result.size(), 1);
    for (OResult d : result) {
      Assert.assertTrue(d.getProperty("a").equals("Ay"));
      Assert.assertTrue(d.getProperty("b").equals("bEE"));
    }

    result = database.command("select 'Ay' as a , 'bEE' as b fetchplan *:1").stream().toList();
    Assert.assertEquals(result.size(), 1);
    for (OResult d : result) {
      Assert.assertTrue(d.getProperty("a").equals("Ay"));
      Assert.assertTrue(d.getProperty("b").equals("bEE"));
    }

    result = database.command("select 'Ay' as a , 'bEE' fetchplan *:1").stream().toList();
    Assert.assertEquals(result.size(), 1);
    for (OResult d : result) {
      Assert.assertTrue(d.getProperty("a").equals("Ay"));
      Assert.assertTrue(d.getProperty("'bEE'").equals("bEE"));
    }
  }

  @Test()
  public void testSelectExcludeFunction() throws IOException {
    try {
      database.command("create class A extends V").close();
      database.command("create class B extends E").close();
      OIdentifiable id =
          database.command("insert into A (a,b) values ('a','b')").next().getIdentity().get();
      OIdentifiable id2 =
          database.command("insert into A (a,b) values ('a','b')").next().getIdentity().get();
      OIdentifiable id3 =
          database.command("insert into A (a,b) values ('a','b')").next().getIdentity().get();
      OIdentifiable id4 =
          database.command("insert into A (a,b) values ('a','b')").next().getIdentity().get();
      database
          .command("create edge B from " + id.getIdentity() + " to " + id2.getIdentity())
          .close();
      database
          .command("create edge B from " + id.getIdentity() + " to " + id3.getIdentity())
          .close();
      database
          .command("create edge B from " + id.getIdentity() + " to " + id4.getIdentity())
          .close();
      database
          .command("create edge B from " + id4.getIdentity() + " to " + id.getIdentity())
          .close();

      List<ODocument> res =
          database.query(
              new OSQLSynchQuery<Object>(
                  "select a,b,in_B.out.exclude('out_B') from "
                      + id2.getIdentity()
                      + " fetchplan in_B.out:1"));

      Assert.assertNotNull(res.get(0).field("a"));
      Assert.assertNotNull(res.get(0).field("b"));
      Assert.assertNull((((List<ODocument>) res.get(0).field("in_B")).get(0).field("out_B")));

      res =
          database.query(
              new OSQLSynchQuery<Object>(
                  "SELECT out.exclude('in_B') FROM ( SELECT EXPAND(in_B) FROM "
                      + id2.getIdentity()
                      + " ) FETCHPLAN out:0 "));

      Assert.assertNotNull(res.get(0).field("out"));
      Assert.assertNotNull(((ODocument) res.get(0).field("out")).field("a"));
      Assert.assertNull(((ODocument) res.get(0).field("out")).field("in_B"));
    } finally {
      database.command("drop class A unsafe ").close();
      database.command("drop class B unsafe ").close();
    }
  }

  @Test
  public void testSimpleExpandExclude() {
    try {
      database.command("create class A extends V").close();
      database.command("create class B extends E").close();
      database.command("create class C extends E").close();
      OIdentifiable id =
          database.command("insert into A (a,b) values ('a1','b1')").next().getIdentity().get();
      OIdentifiable id2 =
          database.command("insert into A (a,b) values ('a2','b2')").next().getIdentity().get();
      OIdentifiable id3 =
          database.command("insert into A (a,b) values ('a3','b3')").next().getIdentity().get();
      database
          .command("create edge B from " + id.getIdentity() + " to " + id2.getIdentity())
          .close();
      database
          .command("create edge C from " + id2.getIdentity() + " to " + id3.getIdentity())
          .close();

      List<ODocument> res =
          database.query(
              new OSQLSynchQuery<Object>(
                  "select out.exclude('in_B') from (select expand(in_C) from "
                      + id3.getIdentity()
                      + " )"));
      Assert.assertEquals(res.size(), 1);
      ODocument ele = res.get(0);
      Assert.assertNotNull(ele.field("out"));
      Assert.assertEquals(((ODocument) ele.field("out")).field("a"), "a2");
      Assert.assertNull(((ODocument) ele.field("out")).field("in_B"));

    } finally {
      database.command("drop class A unsafe ").close();
      database.command("drop class B unsafe ").close();
      database.command("drop class C unsafe ").close();
    }
  }
}
