package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
      //issue #4470
      String className = getClass().getSimpleName() + "_NamedParamsSyntax";
      final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:"+className);
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
      }finally{
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
    } finally {
      db.close();
    }
  }

}
