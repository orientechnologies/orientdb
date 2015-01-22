package com.orientechnologies.orient.core.sql;

import static org.testng.Assert.assertEquals;

import java.util.Set;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

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
}
