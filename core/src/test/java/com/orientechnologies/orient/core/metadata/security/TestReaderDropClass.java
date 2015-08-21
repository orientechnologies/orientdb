package com.orientechnologies.orient.core.metadata.security;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;

public class TestReaderDropClass {

  @Test()
  public void testReaderDropClass() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + TestReaderDropClass.class.getSimpleName());
    db.create();
    try {
      db.getMetadata().getSchema().createClass("Test");
      db.close();
      db.open("reader", "reader");
      try {
        db.getMetadata().getSchema().dropClass("Test");
        Assert.fail("reader should not be able to drop a class");
      } catch (OSecurityAccessException ex) {
      }
      Assert.assertTrue(db.getMetadata().getSchema().existsClass("Test"), "reader should not be able to drop a class");

    } finally {
      db.close();
      db.open("admin", "admin");
      db.drop();
    }
  }

}
