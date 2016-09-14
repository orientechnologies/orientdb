package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OCommandExecutorSQLTruncateTest {

  private ODatabaseDocument database;

  @Before
  public void before() {
    database = new ODatabaseDocumentTx("memory:" + OCommandExecutorSQLTruncateTest.class.getSimpleName());
    database.create();
  }

  @After
  public void after() {
    database.drop();
  }

  @Test
  public void testTruncatePlain() {
    OClass vcl = database.getMetadata().getSchema().createClass("A");
    database.getMetadata().getSchema().createClass("ab", vcl);

    ODocument doc = new ODocument("A");
    database.save(doc);
    doc = new ODocument("ab");
    database.save(doc);
    Number ret = database.command(new OCommandSQL("truncate class A ")).execute();

    assertEquals(ret.intValue(), 1);

  }

  @Test
  public void testTruncatePolimorphic() {
    OClass vcl = database.getMetadata().getSchema().createClass("A");
    database.getMetadata().getSchema().createClass("ab", vcl);

    ODocument doc = new ODocument("A");
    database.save(doc);
    doc = new ODocument("ab");
    database.save(doc);
    Number ret = database.command(new OCommandSQL("truncate class A POLYMORPHIC")).execute();

    assertEquals(ret.intValue(), 2);

  }

}
