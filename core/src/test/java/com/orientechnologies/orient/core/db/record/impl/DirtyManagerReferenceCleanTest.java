package com.orientechnologies.orient.core.db.record.impl;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by tglman on 01/10/15.
 */
public class DirtyManagerReferenceCleanTest {

  private ODatabaseDocument db;

  @Before
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + DirtyManagerReferenceCleanTest.class.getSimpleName());
    db.create();
    db.getMetadata().getSchema().createClass("test");

  }

  @After
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
    assertNull(ORecordInternal.getDirtyManager(doc).getReferences());
    db.commit();
    assertNull(ORecordInternal.getDirtyManager(doc).getReferences());
  }

}
