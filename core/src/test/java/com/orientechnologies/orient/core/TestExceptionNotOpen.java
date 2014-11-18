package com.orientechnologies.orient.core;

import com.orientechnologies.orient.core.exception.OConfigurationException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction;

public class TestExceptionNotOpen {

  @Test
  public void testExceptionNotOpenMemory() {

    ODatabaseDocument db = new ODatabaseDocumentTx("memory:test");
    try {
      db.save(new ODocument());
      Assert.fail();
    } catch (ODatabaseException ex) {
    }
    try {
      db.delete(new ODocument());
      Assert.fail();
    } catch (ODatabaseException ex) {
    }
    try {
      db.hide(new ORecordId());
      Assert.fail();
    } catch (ODatabaseException ex) {
    }
    try {
      db.begin();
      Assert.fail();
    } catch (ODatabaseException ex) {
    }
    try {
      db.begin(OTransaction.TXTYPE.NOTX);
      Assert.fail();
    } catch (ODatabaseException ex) {
    }
    try {
      db.rollback();
      Assert.fail();
    } catch (ODatabaseException ex) {
    }
    try {
      db.commit();
      Assert.fail();
    } catch (ODatabaseException ex) {
    }
    try {
      db.getMetadata();
      Assert.fail();
    } catch (ODatabaseException ex) {
    }

  }

  @Test
  public void testExceptionNotOpenRemote() {
    try {
      ODatabaseDocument db = new ODatabaseDocumentTx("remote:127.0.0.1:00");
      Assert.fail();
    } catch (ODatabaseException e) {
    }
  }

  @Test
  public void testExceptionNotOpenPlocal() {

    ODatabaseDocument db = new ODatabaseDocumentTx("plocal:./target/databaseCheck");
    try {
      db.save(new ODocument());
      Assert.fail();
    } catch (ODatabaseException ex) {
    }
    try {
      db.delete(new ODocument());
      Assert.fail();
    } catch (ODatabaseException ex) {
    }
    try {
      db.hide(new ORecordId());
      Assert.fail();
    } catch (ODatabaseException ex) {
    }
    try {
      db.begin();
      Assert.fail();
    } catch (ODatabaseException ex) {
    }
    try {
      db.begin(OTransaction.TXTYPE.NOTX);
      Assert.fail();
    } catch (ODatabaseException ex) {
    }
    try {
      db.rollback();
      Assert.fail();
    } catch (ODatabaseException ex) {
    }
    try {
      db.commit();
      Assert.fail();
    } catch (ODatabaseException ex) {
    }
    try {
      db.getMetadata();
      Assert.fail();
    } catch (ODatabaseException ex) {
    }
  }

}
