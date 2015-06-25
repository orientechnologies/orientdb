package com.orientechnologies.orient.core.db.record.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;

public class ODirtyManagerTest {

  public ODirtyManagerTest() {
  }

  @Test
  public void testBasicDirtyTracking() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecord().size());
  }

  @Test
  public void testEmbeddedDirtyTracking() {
    ODocument doc = new ODocument();
    ODocument doc1 = new ODocument();
    doc.field("test", doc1, OType.EMBEDDED);
    ODocument doc2 = new ODocument();
    doc1.field("test2", doc2);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecord().size());
    assertEquals(1, manager.getPointed(doc).size());
    assertEquals(doc2, manager.getPointed(doc).get(0));
  }

  @Test
  public void testRefDirtyTracking() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    ODocument doc2 = new ODocument();
    doc.field("test1", doc2);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecord().size());
    assertEquals(1, manager.getPointed(doc).size());
    assertEquals(doc2, manager.getPointed(doc).get(0));
  }

  @Test
  public void testRefDirtyTrackingOther() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    ODocument doc1 = new ODocument();
    doc.field("test1", doc1);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
    assertEquals(2, manager.getNewRecord().size());
    assertEquals(1, manager.getPointed(doc).size());
    assertEquals(doc1, manager.getPointed(doc).get(0));
  }

  @Test
  public void testCollextionRefDirtyTracking() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    List<ODocument> lst = new ArrayList<ODocument>();
    ODocument doc1 = new ODocument();
    lst.add(doc1);
    doc.field("list", lst);

    Set<ODocument> set = new HashSet<ODocument>();
    ODocument doc2 = new ODocument();
    set.add(doc2);
    doc.field("set", set);

    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(3, manager.getNewRecord().size());
    assertEquals(2, manager.getPointed(doc).size());
    assertTrue(manager.getPointed(doc).contains(doc1));
    assertTrue(manager.getPointed(doc).contains(doc2));
  }

  @Test
  public void testCollectionRefDirtyTrackingOther() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    List<ODocument> lst = new ArrayList<ODocument>();
    ODocument doc1 = new ODocument();
    lst.add(doc1);
    doc.field("list", lst);
    Set<ODocument> set = new HashSet<ODocument>();
    ODocument doc2 = new ODocument();
    set.add(doc2);
    doc.field("set", set);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
    ODirtyManager manager2 = ORecordInternal.getDirtyManager(doc2);
    assertTrue(manager2.isSame(manager));
    assertEquals(3, manager.getNewRecord().size());
  }

  @Test
  public void testMapRefDirtyTrackingOther() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    Map<String, ODocument> map = new HashMap<String, ODocument>();
    ODocument doc1 = new ODocument();
    map.put("some", doc1);
    doc.field("list", map);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
    assertEquals(2, manager.getNewRecord().size());
    assertEquals(1, manager.getPointed(doc).size());
    assertTrue(manager.getPointed(doc).contains(doc1));
  }

  @Test
  public void testEmbeddedMapDirtyTrackingOther() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    Map<String, Object> map = new HashMap<String, Object>();
    ODocument doc1 = new ODocument();
    map.put("bla", "bla");
    map.put("some", doc1);
    doc.field("list", map, OType.EMBEDDEDMAP);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecord().size());
  }

  @Test
  public void testEmbeddedCollectionDirtyTrackingOther() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    List<ODocument> lst = new ArrayList<ODocument>();
    ODocument doc1 = new ODocument();
    lst.add(doc1);
    doc.field("list", lst, OType.EMBEDDEDLIST);
    Set<ODocument> set = new HashSet<ODocument>();
    ODocument doc2 = new ODocument();
    set.add(doc2);
    doc.field("set", set, OType.EMBEDDEDSET);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecord().size());
  }

  @Test
  public void testRidBagRefTrackingOther() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    ORidBag bag = new ORidBag();
    ODocument doc1 = new ODocument();
    bag.add(doc1);
    doc.field("bag", bag);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
    assertEquals(2, manager.getNewRecord().size());
  }

}
