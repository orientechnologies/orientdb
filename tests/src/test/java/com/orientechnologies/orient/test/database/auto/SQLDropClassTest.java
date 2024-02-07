package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.testng.Assert;
import org.testng.annotations.Test;

/** Created by luigidellaquila on 09/11/16. */
public class SQLDropClassTest {
  @Test
  public void testSimpleDrop() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + SQLDropClassTest.class.getName());
    db.create();
    try {
      Assert.assertFalse(db.getMetadata().getSchema().existsClass("testSimpleDrop"));
      db.command("create class testSimpleDrop").close();
      Assert.assertTrue(db.getMetadata().getSchema().existsClass("testSimpleDrop"));
      db.command("Drop class testSimpleDrop").close();
      Assert.assertFalse(db.getMetadata().getSchema().existsClass("testSimpleDrop"));
    } finally {
      db.drop();
    }
  }

  @Test
  public void testIfExists() {
    ODatabaseDocument db =
        new ODatabaseDocumentTx("memory:" + SQLDropClassTest.class.getName() + "_ifNotExists");
    db.create();
    try {
      Assert.assertFalse(db.getMetadata().getSchema().existsClass("testIfExists"));
      db.command("create class testIfExists if not exists").close();
      db.getMetadata().getSchema().reload();
      Assert.assertTrue(db.getMetadata().getSchema().existsClass("testIfExists"));
      db.command("drop class testIfExists if exists").close();
      db.getMetadata().getSchema().reload();
      Assert.assertFalse(db.getMetadata().getSchema().existsClass("testIfExists"));
      db.command("drop class testIfExists if exists").close();
      db.getMetadata().getSchema().reload();
      Assert.assertFalse(db.getMetadata().getSchema().existsClass("testIfExists"));

    } finally {
      db.drop();
    }
  }
}
