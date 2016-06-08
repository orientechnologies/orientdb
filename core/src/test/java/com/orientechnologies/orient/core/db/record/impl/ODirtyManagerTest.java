package com.orientechnologies.orient.core.db.record.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.*;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
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
  public void testBasic() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testEmbeddedDocument() {
    ODocument doc = new ODocument();
    ODocument doc1 = new ODocument();
    doc.field("test", doc1, OType.EMBEDDED);
    ODocument doc2 = new ODocument();
    doc1.field("test2", doc2);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
    assertEquals(1, manager.getPointed(doc).size());
    assertEquals(doc2, manager.getPointed(doc).get(0));
  }

  @Test
  public void testLink() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    ODocument doc2 = new ODocument();
    doc.field("test1", doc2);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
    assertEquals(1, manager.getPointed(doc).size());
    assertEquals(doc2, manager.getPointed(doc).get(0));
  }

  @Test
  public void testRemoveLink() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    ODocument doc2 = new ODocument();
    doc.field("test1", doc2);
    doc.removeField("test1");
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
    assertEquals(0, manager.getPointed(doc).size());
  }

  @Test
  public void testSetToNullLink() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    ODocument doc2 = new ODocument();
    doc.field("test1", doc2);
    doc.field("test1", (Object) null);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
    assertEquals(0, manager.getPointed(doc).size());
  }

  @Test
  public void testLinkOther() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    ODocument doc1 = new ODocument();
    doc.field("test1", doc1);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
    assertEquals(2, manager.getNewRecords().size());
    assertEquals(1, manager.getPointed(doc).size());
    assertEquals(doc1, manager.getPointed(doc).get(0));
  }

  @Test
  public void testLinkCollection() {
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
    assertEquals(3, manager.getNewRecords().size());
    assertEquals(2, manager.getPointed(doc).size());
    assertTrue(manager.getPointed(doc).contains(doc1));
    assertTrue(manager.getPointed(doc).contains(doc2));
  }

  @Test
  public void testLinkCollectionRemove() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    List<ODocument> lst = new ArrayList<ODocument>();
    ODocument doc1 = new ODocument();
    lst.add(doc1);
    doc.field("list", lst);
    doc.removeField("list");

    Set<ODocument> set = new HashSet<ODocument>();
    ODocument doc2 = new ODocument();
    set.add(doc2);
    doc.field("set", set);
    doc.removeField("set");

    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecords().size());
    assertEquals(null, manager.getPointed(doc));
  }

  @Test
  public void testLinkCollectionOther() {
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
    assertEquals(3, manager.getNewRecords().size());
  }

  @Test
  public void testLinkMapOther() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    Map<String, ODocument> map = new HashMap<String, ODocument>();
    ODocument doc1 = new ODocument();
    map.put("some", doc1);
    doc.field("list", map);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
    assertEquals(2, manager.getNewRecords().size());
    assertEquals(1, manager.getPointed(doc).size());
    assertTrue(manager.getPointed(doc).contains(doc1));
  }

  @Test
  public void testEmbeddedMap() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    Map<String, Object> map = new HashMap<String, Object>();

    ODocument doc1 = new ODocument();
    map.put("bla", "bla");
    map.put("some", doc1);

    doc.field("list", map, OType.EMBEDDEDMAP);

    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testEmbeddedCollection() {
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
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testRidBag() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    ORidBag bag = new ORidBag();
    ODocument doc1 = new ODocument();
    bag.add(doc1);
    doc.field("bag", bag);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testEmbendedWithEmbeddedCollection() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");

    ODocument emb = new ODocument();
    doc.field("emb", emb, OType.EMBEDDED);

    ODocument embedInList = new ODocument();
    List<ODocument> lst = new ArrayList<ODocument>();
    lst.add(embedInList);
    emb.field("lst", lst, OType.EMBEDDEDLIST);
    ODocument link = new ODocument();
    embedInList.field("set", link);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);

    assertEquals(2, manager.getNewRecords().size());
    assertEquals(1, manager.getPointed(doc).size());
    assertTrue(manager.getPointed(doc).contains(link));
  }

  @Test
  public void testDoubleLevelEmbeddedCollection() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    List<ODocument> lst = new ArrayList<ODocument>();
    ODocument embeddedInList = new ODocument();
    ODocument link = new ODocument();
    embeddedInList.field("link", link);
    lst.add(embeddedInList);
    Set<ODocument> set = new HashSet<ODocument>();
    ODocument embeddedInSet = new ODocument();
    embeddedInSet.field("list", lst, OType.EMBEDDEDLIST);
    set.add(embeddedInSet);
    doc.field("set", set, OType.EMBEDDEDSET);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    ODirtyManager managerNested = ORecordInternal.getDirtyManager(embeddedInSet);
    assertTrue(manager.isSame(managerNested));
    assertEquals(2, manager.getNewRecords().size());
    assertEquals(1, manager.getPointed(doc).size());
    assertTrue(manager.getPointed(doc).contains(link));
  }

  @Test
  public void testDoubleCollectionEmbedded() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    List<ODocument> lst = new ArrayList<ODocument>();
    ODocument embeddedInList = new ODocument();
    ODocument link = new ODocument();
    embeddedInList.field("link", link);
    lst.add(embeddedInList);
    Set<Object> set = new HashSet<Object>();
    set.add(lst);
    doc.field("set", set, OType.EMBEDDEDSET);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
    assertEquals(1, manager.getPointed(doc).size());
    assertTrue(manager.getPointed(doc).contains(link));
  }

  @Test
  public void testDoubleCollectionDocumentEmbedded() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    List<ODocument> lst = new ArrayList<ODocument>();
    ODocument embeddedInList = new ODocument();
    ODocument link = new ODocument();
    ODocument embInDoc = new ODocument();
    embInDoc.field("link", link);
    embeddedInList.field("some", embInDoc, OType.EMBEDDED);
    lst.add(embeddedInList);
    Set<Object> set = new HashSet<Object>();
    set.add(lst);
    doc.field("set", set, OType.EMBEDDEDSET);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
    assertEquals(1, manager.getPointed(doc).size());
    assertTrue(manager.getPointed(doc).contains(link));
  }

  @Test
  public void testDoubleMapEmbedded() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    List<ODocument> lst = new ArrayList<ODocument>();
    ODocument embeddedInList = new ODocument();
    ODocument link = new ODocument();
    embeddedInList.field("link", link);
    lst.add(embeddedInList);
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("some", lst);
    doc.field("set", map, OType.EMBEDDEDMAP);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
    assertEquals(1, manager.getPointed(doc).size());
    assertTrue(manager.getPointed(doc).contains(link));
  }

  @Test
  public void testLinkSet() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    Set<ODocument> set = new HashSet<ODocument>();
    ODocument link = new ODocument();
    set.add(link);
    doc.field("set", set);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
    assertEquals(1, manager.getPointed(doc).size());
    assertTrue(manager.getPointed(doc).contains(link));
  }

  @Test
  public void testLinkSetNoConvert() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    Set<OIdentifiable> set = new ORecordLazySet(doc);
    ODocument link = new ODocument();
    set.add(link);
    doc.field("set", set, OType.LINKSET);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
    assertEquals(1, manager.getPointed(doc).size());
    assertTrue(manager.getPointed(doc).contains(link));
  }

  @Test(enabled = false)
  public void testLinkSetNoConvertRemove() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    Set<OIdentifiable> set = new ORecordLazySet(doc);
    ODocument link = new ODocument();
    set.add(link);
    doc.field("set", set, OType.LINKSET);
    doc.removeField("set");

    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
    assertEquals(0, manager.getPointed(doc).size());
    assertTrue(manager.getPointed(doc).contains(link));
  }

  @Test
  public void testLinkList() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    List<ODocument> list = new ArrayList<ODocument>();
    ODocument link = new ODocument();
    list.add(link);
    doc.field("list", list, OType.LINKLIST);
    ODocument[] linkeds = new ODocument[] { new ODocument().field("name", "linked2"), new ODocument().field("name", "linked3") };
    doc.field("linkeds", linkeds, OType.LINKLIST);

    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(4, manager.getNewRecords().size());
    assertEquals(3, manager.getPointed(doc).size());
    assertTrue(manager.getPointed(doc).contains(link));
  }

  @Test
  public void testLinkMap() {
    ODocument doc = new ODocument();
    doc.field("test", "ddd");
    Map<String, ODocument> map = new HashMap<String, ODocument>();
    ODocument link = new ODocument();
    map.put("bla", link);
    doc.field("map", map, OType.LINKMAP);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
    assertEquals(1, manager.getPointed(doc).size());
    assertTrue(manager.getPointed(doc).contains(link));
  }

  @Test
  public void testNestedMapDocRidBag() {

    ODocument doc = new ODocument();

    Map<String, ODocument> embeddedMap = new HashMap<String, ODocument>();
    ODocument embeddedMapDoc = new ODocument();
    ORidBag embeddedMapDocRidBag = new ORidBag();
    ODocument link = new ODocument();
    embeddedMapDocRidBag.add(link);
    embeddedMapDoc.field("ridBag", embeddedMapDocRidBag);
    embeddedMap.put("k1", embeddedMapDoc);

    doc.field("embeddedMap", embeddedMap, OType.EMBEDDEDMAP);

    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
    assertEquals(1, manager.getPointed(doc).size());
    // TODO: double check this, it's an overhead
    assertEquals(1, manager.getPointed(embeddedMapDoc).size());
    assertTrue(manager.getPointed(doc).contains(link));

  }

  @Test
  public void testLinkListLoop() {
    ODocument doc1 = new ODocument().field("name", "doc1");
    ODocument doc2 = new ODocument().field("name", "doc2");
    ODocument doc3 = new ODocument().field("name", "doc3");
    doc1.field("other", Arrays.asList(doc2, doc3));
    doc2.field("other", Arrays.asList(doc1, doc3));
    doc3.field("other", Arrays.asList(doc1, doc2));
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc1);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
    assertEquals(3, manager.getNewRecords().size());

    assertTrue(manager.getPointed(doc1).contains(doc2));
    assertTrue(manager.getPointed(doc1).contains(doc3));

    assertTrue(manager.getPointed(doc2).contains(doc1));
    assertTrue(manager.getPointed(doc2).contains(doc3));

    assertTrue(manager.getPointed(doc3).contains(doc1));
    assertTrue(manager.getPointed(doc3).contains(doc2));

  }


  @Test
  public void testLinkSetLoop() {
    ODocument doc1 = new ODocument().field("name", "doc1");
    ODocument doc2 = new ODocument().field("name", "doc2");
    ODocument doc3 = new ODocument().field("name", "doc3");
    doc1.field("other", new HashSet(Arrays.asList(doc2, doc3)));
    doc2.field("other", new HashSet(Arrays.asList(doc1, doc3)));
    doc3.field("other", new HashSet(Arrays.asList(doc1, doc2)));
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc1);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
    assertEquals(3, manager.getNewRecords().size());

    assertTrue(manager.getPointed(doc1).contains(doc2));
    assertTrue(manager.getPointed(doc1).contains(doc3));

    assertTrue(manager.getPointed(doc2).contains(doc1));
    assertTrue(manager.getPointed(doc2).contains(doc3));

    assertTrue(manager.getPointed(doc3).contains(doc1));
    assertTrue(manager.getPointed(doc3).contains(doc2));

  }

  @Test
  public void testLinkMapLoop() {
    ODocument doc1 = new ODocument().field("name", "doc1");
    ODocument doc2 = new ODocument().field("name", "doc2");
    ODocument doc3 = new ODocument().field("name", "doc3");
    Map<String, OIdentifiable> map1 = new HashMap<String, OIdentifiable>();
    map1.put("a", doc2);
    map1.put("b", doc3);
    doc1.field("other", map1);
    Map<String, OIdentifiable> map2 = new HashMap<String, OIdentifiable>();
    map2.put("a", doc1);
    map2.put("b", doc3);

    doc2.field("other", map2);
    Map<String, OIdentifiable> map3 = new HashMap<String, OIdentifiable>();
    map3.put("a", doc1);
    map3.put("b", doc2);
    doc3.field("other", map3);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc1);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
    assertEquals(3, manager.getNewRecords().size());

    assertTrue(manager.getPointed(doc1).contains(doc2));
    assertTrue(manager.getPointed(doc1).contains(doc3));

    assertTrue(manager.getPointed(doc2).contains(doc1));
    assertTrue(manager.getPointed(doc2).contains(doc3));

    assertTrue(manager.getPointed(doc3).contains(doc1));
    assertTrue(manager.getPointed(doc3).contains(doc2));

  }

}
