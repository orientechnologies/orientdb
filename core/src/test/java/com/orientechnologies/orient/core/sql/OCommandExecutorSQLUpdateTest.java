package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.annotations.Test;

import java.util.*;

import static org.testng.Assert.*;

public class OCommandExecutorSQLUpdateTest {
  @Test
  public void testUpdateRemoveAll() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OCommandExecutorSQLUpdateTest");
    db.create();

    db.command(new OCommandSQL("CREATE class company")).execute();
    db.command(new OCommandSQL("CREATE property company.name STRING")).execute();
    db.command(new OCommandSQL("CREATE class employee")).execute();
    db.command(new OCommandSQL("CREATE property employee.name STRING")).execute();
    db.command(new OCommandSQL("CREATE property company.employees LINKSET employee")).execute();

    db.command(new OCommandSQL("INSERT INTO company SET name = 'MyCompany'")).execute();

    final ODocument r = (ODocument) db.query(new OSQLSynchQuery<Object>("SELECT FROM company")).get(0);

    db.command(new OCommandSQL("INSERT INTO employee SET name = 'Philipp'")).execute();
    db.command(new OCommandSQL("INSERT INTO employee SET name = 'Selma'")).execute();
    db.command(new OCommandSQL("INSERT INTO employee SET name = 'Thierry'")).execute();
    db.command(new OCommandSQL("INSERT INTO employee SET name = 'Linn'")).execute();

    db.command(new OCommandSQL("UPDATE company ADD employees = (SELECT FROM employee)")).execute();

    r.reload();
    assertEquals(((Set) r.field("employees")).size(), 4);

    db.command(
        new OCommandSQL("UPDATE company REMOVE employees = (SELECT FROM employee WHERE name = 'Linn') WHERE name = 'MyCompany'"))
        .execute();

    r.reload();
    assertEquals(((Set) r.field("employees")).size(), 3);

    db.close();
  }

  @Test
  public void testUpdateContent() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OCommandExecutorSQLUpdateTestContent");
    db.create();
    try {
      db.command(new OCommandSQL("CREATE class V")).execute();
      db.command(new OCommandSQL("insert into V (name) values ('bar')")).execute();
      db.command(new OCommandSQL("UPDATE V content {\"value\":\"foo\"}")).execute();
      Iterable result = db.query(new OSQLSynchQuery<Object>("select from V"));
      ODocument doc = (ODocument) result.iterator().next();
      assertEquals(doc.field("value"), "foo");
    } finally {
      db.close();
    }
  }

  @Test
  public void testUpdateContentParse() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OCommandExecutorSQLUpdateTestContentParse");
    db.create();
    try {
      db.command(new OCommandSQL("CREATE class V")).execute();
      db.command(new OCommandSQL("insert into V (name) values ('bar')")).execute();
      db.command(new OCommandSQL("UPDATE V content {\"value\":\"foo\\\\\"}")).execute();
      Iterable result = db.query(new OSQLSynchQuery<Object>("select from V"));
      ODocument doc = (ODocument) result.iterator().next();
      assertEquals(doc.field("value"), "foo\\");

      db.command(new OCommandSQL("UPDATE V content {\"value\":\"foo\\\\\\\\\"}")).execute();

      result = db.query(new OSQLSynchQuery<Object>("select from V"));
      doc = (ODocument) result.iterator().next();
      assertEquals(doc.field("value"), "foo\\\\");
    } finally {
      db.close();
    }
  }

  @Test
  public void testUpdateMergeWithIndex() {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OCommandExecutorSQLUpdateTestMergeWithIndex");
    db.create();
    try {
      db.command(new OCommandSQL("CREATE CLASS i_have_a_list ")).execute();
      db.command(new OCommandSQL("CREATE PROPERTY i_have_a_list.id STRING")).execute();
      db.command(new OCommandSQL("CREATE INDEX i_have_a_list.id ON i_have_a_list (id) UNIQUE")).execute();
      db.command(new OCommandSQL("CREATE PROPERTY i_have_a_list.types EMBEDDEDLIST STRING")).execute();
      db.command(new OCommandSQL("CREATE INDEX i_have_a_list.types ON i_have_a_list (types) NOTUNIQUE")).execute();
      db.command(new OCommandSQL("INSERT INTO i_have_a_list CONTENT {\"id\": \"the_id\", \"types\": [\"aaa\", \"bbb\"]}"))
          .execute();

      Iterable result = db.query(new OSQLSynchQuery<Object>("SELECT * FROM i_have_a_list WHERE types = 'aaa'"));
      assertTrue(result.iterator().hasNext());

      db.command(
          new OCommandSQL("UPDATE i_have_a_list CONTENT {\"id\": \"the_id\", \"types\": [\"ccc\", \"bbb\"]} WHERE id = 'the_id'"))
          .execute();

      result = db.query(new OSQLSynchQuery<Object>("SELECT * FROM i_have_a_list WHERE types = 'ccc'"));
      assertTrue(result.iterator().hasNext());

      result = db.query(new OSQLSynchQuery<Object>("SELECT * FROM i_have_a_list WHERE types = 'aaa'"));
      assertFalse(result.iterator().hasNext());

    } finally {
      db.close();
    }

  }

  @Test
  public void testNamedParamsSyntax() {
    // issue #4470
    String className = getClass().getSimpleName() + "_NamedParamsSyntax";
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:" + className);
    db.create();

    try {
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

      OCommandSQL sql1 = new OCommandSQL("update " + className
          + " SET name = :name, full_name = :full_name, html_url = :html_url, description = :description, "
          + "git_url = :git_url, ssh_url = :ssh_url, clone_url = :clone_url, svn_url = :svn_url"
          + "UPSERT WHERE full_name = :full_name");
      db.command(sql1).execute(params);

      OCommandSQL sql2 = new OCommandSQL("update " + className
          + " SET name = :name, html_url = :html_url, description = :description, "
          + "git_url = :git_url, ssh_url = :ssh_url, clone_url = :clone_url, svn_url = :svn_url"
          + "UPSERT WHERE full_name = :full_name");
      db.command(sql2).execute(params);
    } finally {
      db.close();
    }
  }

  @Test
  public void testUpsertSetPut() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OCommandExecutorSQLUpdateUpsertSetPut");
    db.create();
    try {
      db.command(new OCommandSQL("CREATE CLASS test")).execute();
      db.command(new OCommandSQL("CREATE PROPERTY test.id integer")).execute();
      db.command(new OCommandSQL("CREATE PROPERTY test.addField EMBEDDEDSET string")).execute();
      db.command(new OCommandSQL("UPDATE test SET id = 1 ADD addField=\"xxxx\" UPSERT WHERE id = 1")).execute();
      Iterable result = db.query(new OSQLSynchQuery<Object>("select from test"));
      ODocument doc = (ODocument) result.iterator().next();
      Set<?> set = doc.field("addField");
      assertEquals(set.size(), 1);
      assertEquals(set.iterator().next(), "xxxx");
    } finally {
      db.close();
    }
  }

  @Test
  public void testUpdateParamDate() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OCommandExecutorSQLUpdateParamDate");
    db.create();
    try {
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
    } finally {
      db.close();
    }
  }

  // issue #4776
  @Test
  public void testBooleanListNamedParameter() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:testBooleanListNamedParameter");
    try {
      ODatabaseRecordThreadLocal.INSTANCE.set(db);
      db.create();
      db.getMetadata().getSchema().createClass("test");

      ODocument doc = new ODocument("test");
      doc.field("id", 1);
      doc.field("boolean", false);
      doc.field("integerList", Collections.EMPTY_LIST);
      doc.field("booleanList", Collections.EMPTY_LIST);
      db.save(doc);

      System.out.println(doc.toJSON());

      OCommandSQL updateCommand = new OCommandSQL(
          "UPDATE test SET boolean = :boolean, booleanList = :booleanList, integerList = :integerList WHERE id = 1");

      Map<String, Object> params = new HashMap<String, Object>();

      // This works.
      params.put("boolean", true);

      // This works
      List<Object> integerList = new ArrayList<Object>();
      integerList.add(1);
      params.put("integerList", integerList);

      // This does not.
      List<Object> booleanList = new ArrayList<Object>();
      booleanList.add(true);
      params.put("booleanList", booleanList);

      db.command(updateCommand).execute(params);

      OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("SELECT * FROM test WHERE id = 1");

      List<ODocument> queryResult = db.command(query).execute(params);
      assertEquals(queryResult.size(), 1);
      ODocument docResult = queryResult.get(0);
      List<?> resultBooleanList = docResult.field("booleanList");
      assertNotNull(resultBooleanList);
      assertEquals(resultBooleanList.size(), 1);
      assertEquals(resultBooleanList.iterator().next(), true);
    } finally {
      db.close();
    }
  }

  @Test
  public void testSingleQuoteInNamedParameter() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OCommandExecutorSQLUpdateTestSingleQuoteInNamedParameter");
    db.create();

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

    db.close();
  }

  @Test
  public void testQuotedStringInNamedParameter() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OCommandExecutorSQLUpdateTestQuotedStringInNamedParameter");
    db.create();

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

    db.close();
  }

  @Test
  public void testSingleQuoteStringInNamedParameter() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OCommandExecutorSQLUpdateTestSingleQuoteStringInNamedParameter");
    db.create();

    db.command(new OCommandSQL("CREATE class test")).execute();

    final ODocument test = new ODocument("test");
    test.field("text", "initial value");

    db.save(test);

    ODocument queried = (ODocument) db.query(new OSQLSynchQuery<Object>("SELECT FROM test")).get(0);
    assertEquals(queried.field("text"), "initial value");

    OCommandSQL command = new OCommandSQL("UPDATE test SET text = :text");
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("text", "quoted 'value' string");

    db.command(command).execute(params);
    queried.reload();
    assertEquals(queried.field("text"), "quoted 'value' string");

    db.close();
  }

  @Test
  public void testQuotesInJson() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OCommandExecutorSQLUpdateTestQuotesInJson");
    db.create();

    db.command(new OCommandSQL("CREATE class testquotesinjson")).execute();
    db.command(new OCommandSQL("UPDATE testquotesinjson SET value = {\"f12\":'test\\\\'} UPSERT WHERE key = \"test\"")).execute();
    // db.command(new OCommandSQL("update V set value.f12 = 'asdf\\\\' WHERE key = \"test\"")).execute();

    ODocument queried = (ODocument) db.query(new OSQLSynchQuery<Object>("SELECT FROM testquotesinjson")).get(0);
    assertEquals(queried.field("value.f12"), "test\\");

    db.close();
  }

  @Test
  public void testDottedTargetInScript() throws Exception {
    //#issue #5397
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OCommandExecutorSQLUpdateTestDottedTargetInScript");
    db.create();

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
    db.close();
  }

  @Test
  public void testBacktickClassName() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OCommandExecutorSQLUpdateTest_testBacktickClassName");
    db.create();
    try {
      db.getMetadata().getSchema().createClass("foo-bar");
      db.command(new OCommandSQL("insert into `foo-bar` set name = 'foo'")).execute();
      db.command(new OCommandSQL("UPDATE `foo-bar` set name = 'bar' where name = 'foo'")).execute();
      Iterable result = db.query(new OSQLSynchQuery<Object>("select from `foo-bar`"));
      ODocument doc = (ODocument) result.iterator().next();
      assertEquals(doc.field("name"), "bar");
    } finally {
      db.close();
    }
  }

  @Test
  public void testUpdateLockLimit() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OCommandExecutorSQLUpdateTest_testUpdateLockLimit");
    db.create();
    try {
      db.getMetadata().getSchema().createClass("foo");
      db.command(new OCommandSQL("insert into foo set name = 'foo'")).execute();
      db.command(new OCommandSQL("UPDATE foo set name = 'bar' where name = 'foo' lock record limit 1")).execute();
      Iterable result = db.query(new OSQLSynchQuery<Object>("select from foo"));
      ODocument doc = (ODocument) result.iterator().next();
      assertEquals(doc.field("name"), "bar");
      db.command(new OCommandSQL("UPDATE foo set name = 'foo' where name = 'bar' lock record limit 1")).execute();
    } finally {
      db.close();
    }
  }


}
