package com.orientechnologies.orient.core.db.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Created by tglman on 01/10/15.
 */
public class DirtyManagerReferenceCleanTest {

  private ODatabaseDocument db;

  @BeforeMethod
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + DirtyManagerReferenceCleanTest.class.getSimpleName());
    db.create();
    db.getMetadata().getSchema().createClass("test");

  }

  @AfterMethod
  public void after() {
    db.drop();
  }

  @Test
  public void testNoTxRefClean() {
    ODocument doc = new ODocument("test");
    ODocument doc1 = new ODocument("test");
    doc.field("ref", doc1);
    assertEquals(1, ORecordInternal.getDirtyManager(doc).getReferences().size());
    db.save(doc);
    assertNull(ORecordInternal.getDirtyManager(doc).getReferences());
  }

  @Test
  public void testTxRefClean() {
    db.begin();
    ODocument doc = new ODocument("test");
    ODocument doc1 = new ODocument("test");
    doc.field("ref", doc1);
    assertEquals(1, ORecordInternal.getDirtyManager(doc).getReferences().size());
    db.save(doc);
    assertEquals(1, ORecordInternal.getDirtyManager(doc).getReferences().size());
    db.commit();
    assertNull(ORecordInternal.getDirtyManager(doc).getReferences());
  }


}
