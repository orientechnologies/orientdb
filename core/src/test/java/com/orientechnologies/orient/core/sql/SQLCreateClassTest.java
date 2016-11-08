package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SQLCreateClassTest {

  @Test public void testSimpleCreate() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + SQLCreateClassTest.class.getName());
    db.create();
    try {
      Assert.assertFalse(db.getMetadata().getSchema().existsClass("testSimpleCreate"));
      db.command(new OCommandSQL("create class testSimpleCreate")).execute();
      Assert.assertTrue(db.getMetadata().getSchema().existsClass("testSimpleCreate"));
    } finally {
      db.drop();
    }
  }

  @Test public void testIfNotExists() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + SQLCreateClassTest.class.getName() + "_ifNotExists");
    db.create();
    try {
      Assert.assertFalse(db.getMetadata().getSchema().existsClass("testIfNotExists"));
      db.command(new OCommandSQL("create class testIfNotExists if not exists")).execute();
      db.getMetadata().getSchema().reload();
      Assert.assertTrue(db.getMetadata().getSchema().existsClass("testIfNotExists"));
      db.command(new OCommandSQL("create class testIfNotExists if not exists")).execute();
      db.getMetadata().getSchema().reload();
      Assert.assertTrue(db.getMetadata().getSchema().existsClass("testIfNotExists"));
      try {
        db.command(new OCommandSQL("create class testIfNotExists")).execute();
        Assert.fail();
      } catch (Exception e) {
      }
    } finally {
      db.drop();
    }
  }

}
