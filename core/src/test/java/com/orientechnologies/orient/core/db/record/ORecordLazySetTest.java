package com.orientechnologies.orient.core.db.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Iterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ORecordLazySetTest {

  private ODatabaseDocumentTx db;
  private ODocument doc1;
  private ODocument doc2;
  private ODocument doc3;
  private ORID rid1;
  private ORID rid2;
  private ORID rid3;

  @Before
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + ORecordLazySet.class.getSimpleName());
    db.create();
    doc1 =
        db.save(
            new ODocument().field("doc1", "doc1"), db.getClusterNameById(db.getDefaultClusterId()));
    rid1 = doc1.getIdentity();
    doc2 =
        db.save(
            new ODocument().field("doc2", "doc2"), db.getClusterNameById(db.getDefaultClusterId()));
    rid2 = doc2.getIdentity();
    doc3 =
        db.save(
            new ODocument().field("doc3", "doc3"), db.getClusterNameById(db.getDefaultClusterId()));
    rid3 = doc3.getIdentity();
  }

  @After
  public void after() {
    db.activateOnCurrentThread();
    db.drop();
  }

  @Test
  public void testConvertToRecord() {
    ORecordLazySet set = new ORecordLazySet(new ODocument());
    set.add(rid1);
    set.add(rid2);
    set.add(rid3);
    set.convertLinks2Records();
    assertEquals(set.size(), 3);
    for (OIdentifiable oIdentifiable : set) {
      assertTrue(oIdentifiable instanceof ODocument);
    }
  }

  @Test
  public void testIteratorConvertToRecord() {
    ORecordLazySet set = new ORecordLazySet(new ODocument());
    set.add(rid1);
    set.add(rid2);
    set.add(rid3);
    for (OIdentifiable oIdentifiable : set) {
      assertTrue(oIdentifiable instanceof ODocument);
    }
    assertEquals(set.size(), 3);
  }

  @Test
  public void testConvertToLink() {
    ORecordLazySet set = new ORecordLazySet(new ODocument());
    set.add(rid1);
    set.add(rid2);
    set.add(rid3);
    for (OIdentifiable oIdentifiable : set) {
      assertTrue(oIdentifiable instanceof ODocument);
    }
    set.convertRecords2Links();
    assertEquals(set.size(), 3);
    Iterator<OIdentifiable> val = set.rawIterator();
    while (val.hasNext()) {
      assertTrue(val.next() instanceof ORecordId);
    }
    assertEquals(set.size(), 3);
  }

  @Test
  @Ignore
  public void testDocumentConvertToLink() {
    ORecordLazySet set = new ORecordLazySet(new ODocument());
    set.add(doc1);
    set.add(doc2);
    set.add(doc3);
    for (OIdentifiable oIdentifiable : set) {
      assertTrue(oIdentifiable instanceof ODocument);
    }
    set.convertRecords2Links();
    assertEquals(set.size(), 3);
    Iterator<OIdentifiable> val = set.rawIterator();
    while (val.hasNext()) {
      assertTrue(val.next() instanceof ORecordId);
    }
    assertEquals(set.size(), 3);
  }

  @Test()
  public void testDocumentNotEmbedded() {
    ORecordLazySet set = new ORecordLazySet(new ODocument());
    ODocument doc = new ODocument();
    set.add(doc);
    assertFalse(doc.isEmbedded());
  }

  @Test()
  public void testSetAddRemove() {
    ORecordLazySet set = new ORecordLazySet(new ODocument());
    ODocument doc = new ODocument();
    set.add(doc);
    set.remove(doc);
    assertTrue(set.isEmpty());
  }

  @Test()
  public void testSetRemoveNotPersistent() {
    ORecordLazySet set = new ORecordLazySet(new ODocument());
    set.add(doc1);
    set.add(doc2);
    set.add(new ORecordId(5, 1000));
    assertEquals(set.size(), 3);
    set.remove(null);
    assertEquals(set.size(), 2);
  }

  @Test
  public void testSetWithNotExistentRecordWithValidation() {
    ODatabaseDocumentTx db =
        new ODatabaseDocumentTx("memory:testSetWithNotExistentRecordWithValidation");
    db.create();
    OClass test = db.getMetadata().getSchema().createClass("test");
    OClass test1 = db.getMetadata().getSchema().createClass("test1");
    test.createProperty("fi", OType.LINKSET).setLinkedClass(test1);
    try {
      ODocument doc = new ODocument(test);
      ORecordLazySet set = new ORecordLazySet(doc);
      set.add(new ORecordId(5, 1000));
      doc.field("fi", set);
      db.begin();
      db.save(doc);
      db.commit();
    } finally {
      db.drop();
    }
  }
}
