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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Test;

public class OCommandExecutorSQLUpdateTest extends BaseMemoryDatabase {

  @Test
  public void testUpdateRemoveAll() throws Exception {

    db.command(new OCommandSQL("CREATE class company")).execute();
    db.command(new OCommandSQL("CREATE property company.name STRING")).execute();
    db.command(new OCommandSQL("CREATE class employee")).execute();
    db.command(new OCommandSQL("CREATE property employee.name STRING")).execute();
    db.command(new OCommandSQL("CREATE property company.employees LINKSET employee")).execute();

    db.command(new OCommandSQL("INSERT INTO company SET name = 'MyCompany'")).execute();

    final ODocument r =
        (ODocument) db.query(new OSQLSynchQuery<Object>("SELECT FROM company")).get(0);

    db.command(new OCommandSQL("INSERT INTO employee SET name = 'Philipp'")).execute();
    db.command(new OCommandSQL("INSERT INTO employee SET name = 'Selma'")).execute();
    db.command(new OCommandSQL("INSERT INTO employee SET name = 'Thierry'")).execute();
    db.command(new OCommandSQL("INSERT INTO employee SET name = 'Linn'")).execute();

    db.command(new OCommandSQL("UPDATE company ADD employees = (SELECT FROM employee)")).execute();

    r.reload();
    assertEquals(((Set) r.field("employees")).size(), 4);

    db.command(
            new OCommandSQL(
                "UPDATE company REMOVE employees = (SELECT FROM employee WHERE name = 'Linn') WHERE name = 'MyCompany'"))
        .execute();

    r.reload();
    assertEquals(((Set) r.field("employees")).size(), 3);
  }

  @Test
  public void testUpdateContent() throws Exception {
    db.command(new OCommandSQL("insert into V (name) values ('bar')")).execute();
    db.command(new OCommandSQL("UPDATE V content {\"value\":\"foo\"}")).execute();
    Iterable result = db.query(new OSQLSynchQuery<Object>("select from V"));
    ODocument doc = (ODocument) result.iterator().next();
    assertEquals(doc.field("value"), "foo");
  }

  @Test
  public void testUpdateContentParse() throws Exception {
    db.command(new OCommandSQL("insert into V (name) values ('bar')")).execute();
    db.command(new OCommandSQL("UPDATE V content {\"value\":\"foo\\\\\"}")).execute();
    Iterable result = db.query(new OSQLSynchQuery<Object>("select from V"));
    ODocument doc = (ODocument) result.iterator().next();
    assertEquals(doc.field("value"), "foo\\");

    db.command(new OCommandSQL("UPDATE V content {\"value\":\"foo\\\\\\\\\"}")).execute();

    result = db.query(new OSQLSynchQuery<Object>("select from V"));
    doc = (ODocument) result.iterator().next();
    assertEquals(doc.field("value"), "foo\\\\");
  }

  @Test
  public void testUpdateMergeWithIndex() {
    db.command(new OCommandSQL("CREATE CLASS i_have_a_list ")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY i_have_a_list.id STRING")).execute();
    db.command(new OCommandSQL("CREATE INDEX i_have_a_list.id ON i_have_a_list (id) UNIQUE"))
        .execute();
    db.command(new OCommandSQL("CREATE PROPERTY i_have_a_list.types EMBEDDEDLIST STRING"))
        .execute();
    db.command(
            new OCommandSQL("CREATE INDEX i_have_a_list.types ON i_have_a_list (types) NOTUNIQUE"))
        .execute();
    db.command(
            new OCommandSQL(
                "INSERT INTO i_have_a_list CONTENT {\"id\": \"the_id\", \"types\": [\"aaa\", \"bbb\"]}"))
        .execute();

    Iterable result =
        db.query(new OSQLSynchQuery<Object>("SELECT * FROM i_have_a_list WHERE types = 'aaa'"));
    assertTrue(result.iterator().hasNext());

    db.command(
            new OCommandSQL(
                "UPDATE i_have_a_list CONTENT {\"id\": \"the_id\", \"types\": [\"ccc\", \"bbb\"]} WHERE id = 'the_id'"))
        .execute();

    result =
        db.query(new OSQLSynchQuery<Object>("SELECT * FROM i_have_a_list WHERE types = 'ccc'"));
    assertTrue(result.iterator().hasNext());

    result =
        db.query(new OSQLSynchQuery<Object>("SELECT * FROM i_have_a_list WHERE types = 'aaa'"));
    assertFalse(result.iterator().hasNext());
  }

  @Test
  public void testNamedParamsSyntax() {
    // issue #4470
    String className = getClass().getSimpleName() + "_NamedParamsSyntax";
    db.command(new OCommandSQL("create class " + className)).execute();

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "foo");
    params.put("full_name", "foo");
    params.put("html_url", "foo");
    params.put("description", "foo");
    params.put("git_url", "foo");
    params.put("ssh_url", "foo");
    params.put("clone_url", "foo");
    params.put("svn_url", "foo");

    OCommandSQL sql1 =
        new OCommandSQL(
            "update "
                + className
                + " SET name = :name, full_name = :full_name, html_url = :html_url, description = :description, "
                + "git_url = :git_url, ssh_url = :ssh_url, clone_url = :clone_url, svn_url = :svn_url"
                + "UPSERT WHERE full_name = :full_name");
    db.command(sql1).execute(params);

    OCommandSQL sql2 =
        new OCommandSQL(
            "update "
                + className
                + " SET name = :name, html_url = :html_url, description = :description, "
                + "git_url = :git_url, ssh_url = :ssh_url, clone_url = :clone_url, svn_url = :svn_url"
                + "UPSERT WHERE full_name = :full_name");
    db.command(sql2).execute(params);
  }

  @Test
  public void testUpsertSetPut() throws Exception {
    db.command(new OCommandSQL("CREATE CLASS test")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY test.id integer")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY test.addField EMBEDDEDSET string")).execute();
    db.command(new OCommandSQL("UPDATE test SET id = 1 ADD addField=\"xxxx\" UPSERT WHERE id = 1"))
        .execute();
    Iterable result = db.query(new OSQLSynchQuery<Object>("select from test"));
    ODocument doc = (ODocument) result.iterator().next();
    Set<?> set = doc.field("addField");
    assertEquals(set.size(), 1);
    assertEquals(set.iterator().next(), "xxxx");
  }

  @Test
  public void testUpdateParamDate() throws Exception {
    db.command(new OCommandSQL("CREATE CLASS test")).execute();
    Date date = new Date();
    db.command(new OCommandSQL("insert into test set birthDate = ?")).execute(date);
    Iterable result = db.query(new OSQLSynchQuery<Object>("select from test"));
    ODocument doc = (ODocument) result.iterator().next();
    assertEquals(doc.field("birthDate"), date);

    date = new Date();
    db.command(new OCommandSQL("UPDATE test set birthDate = ?")).execute(date);
    result = db.query(new OSQLSynchQuery<Object>("select from test"));
    doc = (ODocument) result.iterator().next();
    assertEquals(doc.field("birthDate"), date);
  }

  // issue #4776
  @Test
  public void testBooleanListNamedParameter() {
    db.getMetadata().getSchema().createClass("test");

    ODocument doc = new ODocument("test");
    doc.field("id", 1);
    doc.field("boolean", false);
    doc.field("integerList", Collections.EMPTY_LIST);
    doc.field("booleanList", Collections.EMPTY_LIST);
    db.save(doc);

    OCommandSQL updateCommand =
        new OCommandSQL(
            "UPDATE test SET boolean = :boolean, booleanList = :booleanList, integerList = :integerList WHERE id = 1");

    Map<String, Object> params = new HashMap<String, Object>();

    params.put("boolean", true);

    List<Object> integerList = new ArrayList<Object>();
    integerList.add(1);
    params.put("integerList", integerList);

    List<Object> booleanList = new ArrayList<Object>();
    booleanList.add(true);
    params.put("booleanList", booleanList);

    db.command(updateCommand).execute(params);

    OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>("SELECT * FROM test WHERE id = 1");

    List<ODocument> queryResult = db.command(query).execute(params);
    assertEquals(queryResult.size(), 1);
    ODocument docResult = queryResult.get(0);
    List<?> resultBooleanList = docResult.field("booleanList");
    assertNotNull(resultBooleanList);
    assertEquals(resultBooleanList.size(), 1);
    assertEquals(resultBooleanList.iterator().next(), true);
  }

  @Test
  public void testIncrementWithDotNotationField() throws Exception {
    db.command(new OCommandSQL("CREATE class test")).execute();

    final ODocument test = new ODocument("test");
    test.field("id", "id1");
    test.field("count", 20);

    Map<String, Integer> nestedCound = new HashMap<String, Integer>();
    nestedCound.put("nestedCount", 10);
    test.field("map", nestedCound);

    db.save(test);

    ODocument queried =
        (ODocument)
            db.query(new OSQLSynchQuery<Object>("SELECT FROM test WHERE id = \"id1\"")).get(0);
    ;

    db.command(new OCommandSQL("UPDATE test INCREMENT count = 2")).execute();
    queried.reload();
    //    assertEquals(queried.field("count"), 22);

    Assertions.assertThat(queried.<Integer>field("count")).isEqualTo(22);

    db.command(new OCommandSQL("UPDATE test INCREMENT `map.nestedCount` = 5")).execute();
    queried.reload();
    //    assertEquals(queried.field("map.nestedCount"), 15);

    Assertions.assertThat(queried.<Integer>field("map.nestedCount")).isEqualTo(15);

    db.command(new OCommandSQL("UPDATE test INCREMENT map.nestedCount = 5")).execute();
    queried.reload();

    Assertions.assertThat(queried.<Integer>field("map.nestedCount")).isEqualTo(20);

    //    assertEquals(queried.field("map.nestedCount"), 20);

  }

  @Test
  public void testSingleQuoteInNamedParameter() throws Exception {
    db.command(new OCommandSQL("CREATE class test")).execute();

    final ODocument test = new ODocument("test");
    test.field("text", "initial value");

    db.save(test);

    ODocument queried = (ODocument) db.query(new OSQLSynchQuery<Object>("SELECT FROM test")).get(0);
    assertEquals(queried.field("text"), "initial value");

    OCommandSQL command = new OCommandSQL("UPDATE test SET text = :text");
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("text", "single \"");

    db.command(command).execute(params);
    queried.reload();
    assertEquals(queried.field("text"), "single \"");
  }

  @Test
  public void testQuotedStringInNamedParameter() throws Exception {

    db.command(new OCommandSQL("CREATE class test")).execute();

    final ODocument test = new ODocument("test");
    test.field("text", "initial value");

    db.save(test);

    ODocument queried = (ODocument) db.query(new OSQLSynchQuery<Object>("SELECT FROM test")).get(0);
    assertEquals(queried.field("text"), "initial value");

    OCommandSQL command = new OCommandSQL("UPDATE test SET text = :text");
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("text", "quoted \"value\" string");

    db.command(command).execute(params);
    queried.reload();
    assertEquals(queried.field("text"), "quoted \"value\" string");
  }

  @Test
  public void testQuotesInJson() throws Exception {

    db.command(new OCommandSQL("CREATE class testquotesinjson")).execute();
    db.command(
            new OCommandSQL(
                "UPDATE testquotesinjson SET value = {\"f12\":'test\\\\'} UPSERT WHERE key = \"test\""))
        .execute();
    // db.command(new OCommandSQL("update V set value.f12 = 'asdf\\\\' WHERE key =
    // \"test\"")).execute();

    ODocument queried =
        (ODocument) db.query(new OSQLSynchQuery<Object>("SELECT FROM testquotesinjson")).get(0);
    assertEquals(queried.field("value.f12"), "test\\");
  }

  @Test
  public void testDottedTargetInScript() throws Exception {
    // #issue #5397

    db.command(new OCommandSQL("create class A")).execute();
    db.command(new OCommandSQL("create class B")).execute();
    db.command(new OCommandSQL("insert into A set name = 'foo'")).execute();
    db.command(new OCommandSQL("insert into B set name = 'bar', a = (select from A)")).execute();

    StringBuilder script = new StringBuilder();
    script.append("let $a = select from B;\n");
    script.append("update $a.a set name = 'baz';\n");
    db.command(new OCommandScript(script.toString())).execute();

    List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select from A"));
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).field("name"), "baz");
  }

  @Test
  public void testBacktickClassName() throws Exception {
    db.getMetadata().getSchema().createClass("foo-bar");
    db.command(new OCommandSQL("insert into `foo-bar` set name = 'foo'")).execute();
    db.command(new OCommandSQL("UPDATE `foo-bar` set name = 'bar' where name = 'foo'")).execute();
    Iterable result = db.query(new OSQLSynchQuery<Object>("select from `foo-bar`"));
    ODocument doc = (ODocument) result.iterator().next();
    assertEquals(doc.field("name"), "bar");
  }

  @Test
  @Ignore
  public void testUpdateLockLimit() throws Exception {
    db.getMetadata().getSchema().createClass("foo");
    db.command(new OCommandSQL("insert into foo set name = 'foo'")).execute();
    db.command(
            new OCommandSQL("UPDATE foo set name = 'bar' where name = 'foo' lock record limit 1"))
        .execute();
    Iterable result = db.query(new OSQLSynchQuery<Object>("select from foo"));
    ODocument doc = (ODocument) result.iterator().next();
    assertEquals(doc.field("name"), "bar");
    db.command(
            new OCommandSQL("UPDATE foo set name = 'foo' where name = 'bar' lock record limit 1"))
        .execute();
  }

  @Test
  public void testUpdateContentOnClusterTarget() throws Exception {
    db.command(new OCommandSQL("CREATE class Foo")).execute();
    db.command(new OCommandSQL("CREATE class Bar")).execute();
    db.command(new OCommandSQL("CREATE property Foo.bar EMBEDDED Bar")).execute();

    db.command(new OCommandSQL("insert into cluster:foo set bar = {\"value\":\"zz\\\\\"}"))
        .execute();
    db.command(new OCommandSQL("UPDATE cluster:foo set bar = {\"value\":\"foo\\\\\"}")).execute();
    Iterable result = db.query(new OSQLSynchQuery<Object>("select from cluster:foo"));
    ODocument doc = (ODocument) result.iterator().next();
    assertEquals(((ODocument) doc.field("bar")).field("value"), "foo\\");
  }

  @Test
  public void testUpdateContentOnClusterTargetMultiple() throws Exception {
    db.command(new OCommandSQL("CREATE class Foo")).execute();
    db.command(new OCommandSQL("ALTER CLASS Foo addcluster fooadditional1")).execute();
    db.command(new OCommandSQL("ALTER CLASS Foo addcluster fooadditional2")).execute();
    db.command(new OCommandSQL("CREATE class Bar")).execute();
    db.command(new OCommandSQL("CREATE property Foo.bar EMBEDDED Bar")).execute();

    db.command(new OCommandSQL("insert into cluster:foo set bar = {\"value\":\"zz\\\\\"}"))
        .execute();
    db.command(new OCommandSQL("UPDATE cluster:foo set bar = {\"value\":\"foo\\\\\"}")).execute();
    Iterable result = db.query(new OSQLSynchQuery<Object>("select from cluster:foo"));
    Iterator iterator = result.iterator();
    assertTrue(iterator.hasNext());
    ODocument doc = (ODocument) iterator.next();
    assertEquals(((ODocument) doc.field("bar")).field("value"), "foo\\");
    assertFalse(iterator.hasNext());

    db.command(
            new OCommandSQL("insert into cluster:fooadditional1 set bar = {\"value\":\"zz\\\\\"}"))
        .execute();
    db.command(new OCommandSQL("UPDATE cluster:fooadditional1 set bar = {\"value\":\"foo\\\\\"}"))
        .execute();
    result = db.query(new OSQLSynchQuery<Object>("select from cluster:fooadditional1"));
    iterator = result.iterator();
    assertTrue(iterator.hasNext());
    doc = (ODocument) iterator.next();
    assertEquals(((ODocument) doc.field("bar")).field("value"), "foo\\");
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testUpdateContentOnClusterTargetMultipleSelection() throws Exception {
    db.command(new OCommandSQL("CREATE class Foo")).execute();
    db.command(new OCommandSQL("ALTER CLASS Foo addcluster fooadditional1")).execute();
    db.command(new OCommandSQL("ALTER CLASS Foo addcluster fooadditional2")).execute();
    db.command(new OCommandSQL("ALTER CLASS Foo addcluster fooadditional3")).execute();
    db.command(new OCommandSQL("CREATE class Bar")).execute();
    db.command(new OCommandSQL("CREATE property Foo.bar EMBEDDED Bar")).execute();

    db.command(
            new OCommandSQL("insert into cluster:fooadditional1 set bar = {\"value\":\"zz\\\\\"}"))
        .execute();
    db.command(
            new OCommandSQL("insert into cluster:fooadditional2 set bar = {\"value\":\"zz\\\\\"}"))
        .execute();
    db.command(
            new OCommandSQL("insert into cluster:fooadditional3 set bar = {\"value\":\"zz\\\\\"}"))
        .execute();
    db.command(
            new OCommandSQL(
                "UPDATE cluster:[fooadditional1, fooadditional2] set bar = {\"value\":\"foo\\\\\"}"))
        .execute();
    List<?> result =
        db.query(
            new OSQLSynchQuery<Object>("select from cluster:[ fooadditional1, fooadditional2 ]"));
    Iterator<?> iterator = result.iterator();
    assertTrue(iterator.hasNext());
    ODocument doc = (ODocument) iterator.next();
    assertEquals(((ODocument) doc.field("bar")).field("value"), "foo\\");
    assertTrue(iterator.hasNext());
    doc = (ODocument) iterator.next();
    assertEquals(((ODocument) doc.field("bar")).field("value"), "foo\\");
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testUpdateContentNotORestricted() throws Exception {
    // issue #5564
    db.command(new OCommandSQL("CREATE class Foo")).execute();

    ODocument d = new ODocument("Foo");
    d.field("name", "foo");
    d.save();
    db.command(new OCommandSQL("update Foo MERGE {\"a\":1}")).execute();
    db.command(new OCommandSQL("update Foo CONTENT {\"a\":1}")).execute();

    List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select from Foo"));

    assertEquals(result.size(), 1);
    ODocument doc = result.get(0);
    assertNull(doc.field("_allowRead"));
  }

  @Test
  public void testUpdateReturnCount() throws Exception {
    // issue #5564
    db.command(new OCommandSQL("CREATE class Foo")).execute();

    ODocument d = new ODocument("Foo");
    d.field("name", "foo");
    d.save();
    d = new ODocument("Foo");
    d.field("name", "bar");
    d.save();

    Object result =
        db.command(new OCommandSQL("update Foo set surname = 'baz' return count")).execute();

    assertEquals(2, result);
  }

  @Test
  public void testLinkedUpdate() {
    db.command("CREATE class TestSource").close();
    db.command("CREATE class TestLinked").close();
    db.command("CREATE property TestLinked.id STRING").close();
    db.command("CREATE INDEX TestLinked.id ON TestLinked (id) UNIQUE_HASH_INDEX ENGINE HASH_INDEX")
        .close();

    ODocument state = new ODocument("TestLinked");
    state.setProperty("id", "idvalue");
    db.save(state);

    ODocument d = new ODocument("TestSource");
    d.setProperty("name", "foo");
    d.setProperty("linked", state);
    db.save(d);

    ((ODatabaseDocumentInternal) db).getLocalCache().clear();

    db.command(
            "Update TestSource set flag = true , linked.flag = true return after *, linked:{*} as infoLinked  where name = \"foo\"")
        .close();
    ((ODatabaseDocumentInternal) db).getLocalCache().clear();

    OResultSet result = db.query("select from TestLinked where id = \"idvalue\"");
    while (result.hasNext()) {
      OResult res = result.next();
      assertTrue(res.hasProperty("flag"));
      assertTrue((Boolean) res.getProperty("flag"));
    }
  }
}
