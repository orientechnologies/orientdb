package com.orientechnologies.orient.core.db.record.ridbag;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public abstract class ORidBagTest {
  protected ODatabaseDocumentTx db;

  @BeforeClass
  public void setUp() throws Exception {
    db = new ODatabaseDocumentTx("plocal:target/testdb/OSBTreeRidBagTest");
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();
  }

  @AfterClass
  public void tearDown() throws Exception {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
    db.drop();
  }

  public void testAdd() throws Exception {
    ORidBag bag = new ORidBag();
    bag.setAutoConvertToRecord(false);

    bag.add(new ORecordId("#77:1"));
    Iterator<OIdentifiable> iterator = bag.iterator();
    Assert.assertTrue(iterator.hasNext());

    OIdentifiable identifiable = iterator.next();
    Assert.assertEquals(identifiable, new ORecordId("#77:1"));

    Assert.assertTrue(!iterator.hasNext());
    assertEmbedded(bag.isEmbedded());
  }

  public void testAdd2() throws Exception {
    ORidBag bag = new ORidBag();
    bag.setAutoConvertToRecord(false);

    bag.add(new ORecordId("#77:2"));
    bag.add(new ORecordId("#77:2"));
    assertEquals(bag.size(), 2);
    assertEmbedded(bag.isEmbedded());
  }

  public void testAddRemove() {
    ORidBag bag = new ORidBag();
    bag.setAutoConvertToRecord(false);

    bag.add(new ORecordId("#77:2"));
    bag.add(new ORecordId("#77:2"));
    bag.add(new ORecordId("#77:3"));
    bag.add(new ORecordId("#77:4"));
    bag.add(new ORecordId("#77:4"));
    bag.add(new ORecordId("#77:4"));
    bag.add(new ORecordId("#77:5"));
    bag.add(new ORecordId("#77:6"));

    bag.remove(new ORecordId("#77:1"));
    bag.remove(new ORecordId("#77:2"));
    bag.remove(new ORecordId("#77:2"));
    bag.remove(new ORecordId("#77:4"));
    bag.remove(new ORecordId("#77:6"));

    assertEmbedded(bag.isEmbedded());

    final List<OIdentifiable> rids = new ArrayList<OIdentifiable>();
    rids.add(new ORecordId("#77:3"));
    rids.add(new ORecordId("#77:4"));
    rids.add(new ORecordId("#77:4"));
    rids.add(new ORecordId("#77:5"));

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());

    for (OIdentifiable identifiable : bag)
      rids.add(identifiable);

    ODocument doc = new ODocument();
    doc.field("ridbag", bag);
    doc.save();

    ORID rid = doc.getIdentity();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());
  }

  public void testAddRemoveSBTreeContainsValues() {
    ORidBag bag = new ORidBag();
    bag.setAutoConvertToRecord(false);

    bag.add(new ORecordId("#77:2"));
    bag.add(new ORecordId("#77:2"));
    bag.add(new ORecordId("#77:3"));
    bag.add(new ORecordId("#77:4"));
    bag.add(new ORecordId("#77:4"));
    bag.add(new ORecordId("#77:4"));
    bag.add(new ORecordId("#77:5"));
    bag.add(new ORecordId("#77:6"));

    assertEmbedded(bag.isEmbedded());

    ODocument doc = new ODocument();
    doc.field("ridbag", bag);
    doc.save();

    ORID rid = doc.getIdentity();

    OStorage storage = db.getStorage();
    db.close();
    storage.close(true);

    db.open("admin", "admin");

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    bag.remove(new ORecordId("#77:1"));
    bag.remove(new ORecordId("#77:2"));
    bag.remove(new ORecordId("#77:2"));
    bag.remove(new ORecordId("#77:4"));
    bag.remove(new ORecordId("#77:6"));

    final List<OIdentifiable> rids = new ArrayList<OIdentifiable>();
    rids.add(new ORecordId("#77:3"));
    rids.add(new ORecordId("#77:4"));
    rids.add(new ORecordId("#77:4"));
    rids.add(new ORecordId("#77:5"));

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());

    for (OIdentifiable identifiable : bag)
      rids.add(identifiable);

    doc = new ODocument();
    ORidBag otherBag = new ORidBag();
    for (OIdentifiable id : bag)
      otherBag.add(id);

    assertEmbedded(otherBag.isEmbedded());
    doc.field("ridbag", otherBag);
    doc.save();

    rid = doc.getIdentity();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());
  }

  public void testAddRemoveDuringIterationSBTreeContainsValues() {
    ORidBag bag = new ORidBag();
    bag.setAutoConvertToRecord(false);
    assertEmbedded(bag.isEmbedded());

    bag.add(new ORecordId("#77:2"));
    bag.add(new ORecordId("#77:2"));
    bag.add(new ORecordId("#77:3"));
    bag.add(new ORecordId("#77:4"));
    bag.add(new ORecordId("#77:4"));
    bag.add(new ORecordId("#77:4"));
    bag.add(new ORecordId("#77:5"));
    bag.add(new ORecordId("#77:6"));
    assertEmbedded(bag.isEmbedded());

    ODocument doc = new ODocument();
    doc.field("ridbag", bag);
    doc.save();

    ORID rid = doc.getIdentity();

    OStorage storage = db.getStorage();
    db.close();
    storage.close(true);

    db.open("admin", "admin");

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    bag.remove(new ORecordId("#77:1"));
    bag.remove(new ORecordId("#77:2"));
    bag.remove(new ORecordId("#77:2"));
    bag.remove(new ORecordId("#77:4"));
    bag.remove(new ORecordId("#77:6"));
    assertEmbedded(bag.isEmbedded());

    final List<OIdentifiable> rids = new ArrayList<OIdentifiable>();
    rids.add(new ORecordId("#77:3"));
    rids.add(new ORecordId("#77:4"));
    rids.add(new ORecordId("#77:4"));
    rids.add(new ORecordId("#77:5"));

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());

    for (OIdentifiable identifiable : bag)
      rids.add(identifiable);

    Iterator<OIdentifiable> iterator = bag.iterator();
    while (iterator.hasNext()) {
      final OIdentifiable identifiable = iterator.next();
      if (identifiable.equals(new ORecordId("#77:4"))) {
        iterator.remove();
        assertTrue(rids.remove(identifiable));
      }
    }

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    for (OIdentifiable identifiable : bag)
      rids.add(identifiable);

    assertEmbedded(bag.isEmbedded());
    doc = new ODocument();

    final ORidBag otherBag = new ORidBag();
    for (OIdentifiable id : bag)
      otherBag.add(id);

    assertEmbedded(otherBag.isEmbedded());
    doc.field("ridbag", otherBag);
    doc.save();

    rid = doc.getIdentity();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());
  }

  public void testEmptyIterator() throws Exception {
    ORidBag bag = new ORidBag();
    bag.setAutoConvertToRecord(false);
    assertEmbedded(bag.isEmbedded());
    assertEquals(bag.size(), 0);

    for (OIdentifiable id : bag) {
      Assert.fail();
    }
  }

  public void testAddRemoveNotExisting() {
    List<OIdentifiable> rids = new ArrayList<OIdentifiable>();

    ORidBag bag = new ORidBag();
    assertEmbedded(bag.isEmbedded());
    bag.setAutoConvertToRecord(false);

    bag.add(new ORecordId("#77:2"));
    rids.add(new ORecordId("#77:2"));

    bag.add(new ORecordId("#77:2"));
    rids.add(new ORecordId("#77:2"));

    bag.add(new ORecordId("#77:3"));
    rids.add(new ORecordId("#77:3"));

    bag.add(new ORecordId("#77:4"));
    rids.add(new ORecordId("#77:4"));

    bag.add(new ORecordId("#77:4"));
    rids.add(new ORecordId("#77:4"));

    bag.add(new ORecordId("#77:4"));
    rids.add(new ORecordId("#77:4"));

    bag.add(new ORecordId("#77:5"));
    rids.add(new ORecordId("#77:5"));

    bag.add(new ORecordId("#77:6"));
    rids.add(new ORecordId("#77:6"));
    assertEmbedded(bag.isEmbedded());

    ODocument doc = new ODocument();
    doc.field("ridbag", bag);
    doc.save();

    ORID rid = doc.getIdentity();

    OStorage storage = db.getStorage();
    db.close();
    storage.close(true);

    db.open("admin", "admin");

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    bag.add(new ORecordId("#77:2"));
    rids.add(new ORecordId("#77:2"));

    bag.remove(new ORecordId("#77:4"));
    rids.remove(new ORecordId("#77:4"));

    bag.remove(new ORecordId("#77:4"));
    rids.remove(new ORecordId("#77:4"));

    bag.remove(new ORecordId("#77:2"));
    rids.remove(new ORecordId("#77:2"));

    bag.remove(new ORecordId("#77:2"));
    rids.remove(new ORecordId("#77:2"));

    bag.remove(new ORecordId("#77:7"));
    rids.remove(new ORecordId("#77:7"));

    bag.remove(new ORecordId("#77:8"));
    rids.remove(new ORecordId("#77:8"));

    bag.remove(new ORecordId("#77:8"));
    rids.remove(new ORecordId("#77:8"));

    bag.remove(new ORecordId("#77:8"));
    rids.remove(new ORecordId("#77:8"));

    assertEmbedded(bag.isEmbedded());

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());

    for (OIdentifiable identifiable : bag)
      rids.add(identifiable);

    doc.save();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());

  }

  public void testAddAllAndIterator() throws Exception {
    final Set<OIdentifiable> expected = new HashSet<OIdentifiable>(8);

    expected.add(new ORecordId("#77:12"));
    expected.add(new ORecordId("#77:13"));
    expected.add(new ORecordId("#77:14"));
    expected.add(new ORecordId("#77:15"));
    expected.add(new ORecordId("#77:16"));

    ORidBag bag = new ORidBag();
    bag.setAutoConvertToRecord(false);

    bag.addAll(expected);
    assertEmbedded(bag.isEmbedded());

    assertEquals(bag.size(), 5);

    Set<OIdentifiable> actual = new HashSet<OIdentifiable>(8);
    for (OIdentifiable id : bag) {
      actual.add(id);
    }

    assertEquals(actual, expected);
  }

  public void testAddSBTreeAddInMemoryIterate() {
    List<OIdentifiable> rids = new ArrayList<OIdentifiable>();

    ORidBag bag = new ORidBag();
    assertEmbedded(bag.isEmbedded());
    bag.setAutoConvertToRecord(false);

    bag.add(new ORecordId("#77:2"));
    rids.add(new ORecordId("#77:2"));

    bag.add(new ORecordId("#77:2"));
    rids.add(new ORecordId("#77:2"));

    bag.add(new ORecordId("#77:3"));
    rids.add(new ORecordId("#77:3"));

    bag.add(new ORecordId("#77:4"));
    rids.add(new ORecordId("#77:4"));

    bag.add(new ORecordId("#77:4"));
    rids.add(new ORecordId("#77:4"));
    assertEmbedded(bag.isEmbedded());

    ODocument doc = new ODocument();
    doc.field("ridbag", bag);
    doc.save();

    ORID rid = doc.getIdentity();

    OStorage storage = db.getStorage();
    db.close();
    storage.close(true);

    db.open("admin", "admin");

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    bag.add(new ORecordId("#77:0"));
    rids.add(new ORecordId("#77:0"));

    bag.add(new ORecordId("#77:1"));
    rids.add(new ORecordId("#77:1"));

    bag.add(new ORecordId("#77:2"));
    rids.add(new ORecordId("#77:2"));

    bag.add(new ORecordId("#77:3"));
    rids.add(new ORecordId("#77:3"));

    bag.add(new ORecordId("#77:5"));
    rids.add(new ORecordId("#77:5"));

    bag.add(new ORecordId("#77:6"));
    rids.add(new ORecordId("#77:6"));

    assertEmbedded(bag.isEmbedded());

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());

    for (OIdentifiable identifiable : bag)
      rids.add(identifiable);

    doc = new ODocument();
    final ORidBag otherBag = new ORidBag();
    for (OIdentifiable id : bag)
      otherBag.add(id);

    doc.field("ridbag", otherBag);
    doc.save();

    rid = doc.getIdentity();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());
  }

  public void testAddSBTreeAddInMemoryIterateAndRemove() {
    List<OIdentifiable> rids = new ArrayList<OIdentifiable>();

    ORidBag bag = new ORidBag();
    bag.setAutoConvertToRecord(false);
    assertEmbedded(bag.isEmbedded());

    bag.add(new ORecordId("#77:2"));
    rids.add(new ORecordId("#77:2"));

    bag.add(new ORecordId("#77:2"));
    rids.add(new ORecordId("#77:2"));

    bag.add(new ORecordId("#77:3"));
    rids.add(new ORecordId("#77:3"));

    bag.add(new ORecordId("#77:4"));
    rids.add(new ORecordId("#77:4"));

    bag.add(new ORecordId("#77:4"));
    rids.add(new ORecordId("#77:4"));

    bag.add(new ORecordId("#77:7"));
    rids.add(new ORecordId("#77:7"));

    bag.add(new ORecordId("#77:8"));
    rids.add(new ORecordId("#77:8"));

    assertEmbedded(bag.isEmbedded());

    ODocument doc = new ODocument();
    doc.field("ridbag", bag);
    doc.save();

    ORID rid = doc.getIdentity();

    OStorage storage = db.getStorage();
    db.close();
    storage.close(true);

    db.open("admin", "admin");

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    bag.add(new ORecordId("#77:0"));
    rids.add(new ORecordId("#77:0"));

    bag.add(new ORecordId("#77:1"));
    rids.add(new ORecordId("#77:1"));

    bag.add(new ORecordId("#77:2"));
    rids.add(new ORecordId("#77:2"));

    bag.add(new ORecordId("#77:3"));
    rids.add(new ORecordId("#77:3"));

    bag.add(new ORecordId("#77:3"));
    rids.add(new ORecordId("#77:3"));

    bag.add(new ORecordId("#77:5"));
    rids.add(new ORecordId("#77:5"));

    bag.add(new ORecordId("#77:6"));
    rids.add(new ORecordId("#77:6"));

    assertEmbedded(bag.isEmbedded());

    Iterator<OIdentifiable> iterator = bag.iterator();
    int r2c = 0;
    int r3c = 0;
    int r6c = 0;
    int r4c = 0;
    int r7c = 0;

    while (iterator.hasNext()) {
      OIdentifiable identifiable = iterator.next();
      if (identifiable.equals(new ORecordId("#77:2"))) {
        if (r2c < 2) {
          r2c++;
          iterator.remove();
          rids.remove(identifiable);
        }
      }

      if (identifiable.equals(new ORecordId("#77:3"))) {
        if (r3c < 1) {
          r3c++;
          iterator.remove();
          rids.remove(identifiable);
        }
      }

      if (identifiable.equals(new ORecordId("#77:6"))) {
        if (r6c < 1) {
          r6c++;
          iterator.remove();
          rids.remove(identifiable);
        }
      }

      if (identifiable.equals(new ORecordId("#77:4"))) {
        if (r4c < 1) {
          r4c++;
          iterator.remove();
          rids.remove(identifiable);
        }
      }

      if (identifiable.equals(new ORecordId("#77:7"))) {
        if (r7c < 1) {
          r7c++;
          iterator.remove();
          rids.remove(identifiable);
        }
      }
    }

    assertEquals(r2c, 2);
    assertEquals(r3c, 1);
    assertEquals(r6c, 1);
    assertEquals(r4c, 1);
    assertEquals(r7c, 1);

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());

    for (OIdentifiable identifiable : bag)
      rids.add(identifiable);

    doc = new ODocument();

    final ORidBag otherBag = new ORidBag();
    for (OIdentifiable id : bag)
      otherBag.add(id);

    assertEmbedded(otherBag.isEmbedded());

    doc.field("ridbag", otherBag);
    doc.save();

    rid = doc.getIdentity();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());
  }

  public void testRemove() {
    final Set<OIdentifiable> expected = new HashSet<OIdentifiable>(8);

    expected.add(new ORecordId("#77:12"));
    expected.add(new ORecordId("#77:13"));
    expected.add(new ORecordId("#77:14"));
    expected.add(new ORecordId("#77:15"));
    expected.add(new ORecordId("#77:16"));

    final ORidBag bag = new ORidBag();
    assertEmbedded(bag.isEmbedded());
    bag.setAutoConvertToRecord(false);
    bag.addAll(expected);
    assertEmbedded(bag.isEmbedded());

    bag.remove(new ORecordId("#77:23"));
    assertEmbedded(bag.isEmbedded());

    final Set<OIdentifiable> expectedTwo = new HashSet<OIdentifiable>(8);
    expectedTwo.addAll(expected);

    for (OIdentifiable identifiable : bag) {
      assertTrue(expectedTwo.remove(identifiable));
    }

    Assert.assertTrue(expectedTwo.isEmpty());

    expected.remove(new ORecordId("#77:14"));
    bag.remove(new ORecordId("#77:14"));
    assertEmbedded(bag.isEmbedded());

    expectedTwo.addAll(expected);

    for (OIdentifiable identifiable : bag) {
      assertTrue(expectedTwo.remove(identifiable));
    }
  }

  public void testSaveLoad() throws Exception {
    Set<OIdentifiable> expected = new HashSet<OIdentifiable>(8);

    expected.add(new ORecordId("#77:12"));
    expected.add(new ORecordId("#77:13"));
    expected.add(new ORecordId("#77:14"));
    expected.add(new ORecordId("#77:15"));
    expected.add(new ORecordId("#77:16"));
    expected.add(new ORecordId("#77:17"));
    expected.add(new ORecordId("#77:18"));
    expected.add(new ORecordId("#77:19"));
    expected.add(new ORecordId("#77:20"));
    expected.add(new ORecordId("#77:21"));
    expected.add(new ORecordId("#77:22"));

    ODocument doc = new ODocument();

    final ORidBag bag = new ORidBag();
    bag.addAll(expected);

    doc.field("ridbag", bag);
    assertEmbedded(bag.isEmbedded());
    doc.save();
    final ORID id = doc.getIdentity();

    OStorage storage = db.getStorage();
    db.close();
    storage.close(true);

    db.open("admin", "admin");

    doc = db.load(id);
    doc.setLazyLoad(false);

    final ORidBag loaded = doc.field("ridbag");
    assertEmbedded(loaded.isEmbedded());

    Assert.assertEquals(loaded.size(), expected.size());
    for (OIdentifiable identifiable : loaded)
      Assert.assertTrue(expected.remove(identifiable));

    Assert.assertTrue(expected.isEmpty());
  }

  public void testSaveInBackOrder() throws Exception {
    ODocument docA = new ODocument().field("name", "A");
    ODocument docB = new ODocument().field("name", "B").save();

    ORidBag ridBag = new ORidBag();

    ridBag.add(docA);
    ridBag.add(docB);

    docA.save();
    ridBag.remove(docB);

    assertEmbedded(ridBag.isEmbedded());

    HashSet<OIdentifiable> result = new HashSet<OIdentifiable>();

    for (OIdentifiable oIdentifiable : ridBag) {
      result.add(oIdentifiable);
    }

    Assert.assertTrue(result.contains(docA));
    Assert.assertFalse(result.contains(docB));
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(1, ridBag.size());
  }

  public void testMassiveChanges() {
    ODocument document = new ODocument();
    ORidBag bag = new ORidBag();
    assertEmbedded(bag.isEmbedded());

    Random random = new Random();
    List<OIdentifiable> rids = new ArrayList<OIdentifiable>();
    document.field("bag", bag);
    document.save();
    ORID rid = document.getIdentity();

    for (int i = 0; i < 10; i++) {
      document = db.load(rid);
      document.setLazyLoad(false);

      bag = document.field("bag");
      assertEmbedded(bag.isEmbedded());

      massiveInsertionIteration(random, rids, bag);
      assertEmbedded(bag.isEmbedded());

      document.save();
    }
    document.delete();
  }

  public void testSimultaneousIterationAndRemove() {
    ORidBag ridBag = new ORidBag();
    ODocument document = new ODocument();
    document.field("ridBag", ridBag);
    assertEmbedded(ridBag.isEmbedded());

    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save();

      ridBag.add(docToAdd);
    }

    assertEmbedded(ridBag.isEmbedded());
    document.save();

    document.reload();
    ridBag = document.field("ridBag");

    Set<OIdentifiable> docs = Collections.newSetFromMap(new IdentityHashMap<OIdentifiable, Boolean>());
    for (OIdentifiable id : ridBag)
      docs.add(id);

    db.begin();
    ridBag = document.field("ridBag");
    assertEmbedded(ridBag.isEmbedded());

    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save();

      docs.add(docToAdd);
      ridBag.add(docToAdd);
    }

    assertEmbedded(ridBag.isEmbedded());

    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();

      docs.add(docToAdd);
      ridBag.add(docToAdd);
    }

    assertEmbedded(ridBag.isEmbedded());
    for (OIdentifiable identifiable : ridBag) {
      Assert.assertTrue(docs.remove(identifiable));
      ridBag.remove(identifiable);
      Assert.assertEquals(ridBag.size(), docs.size());

      int counter = 0;
      for (OIdentifiable id : ridBag) {
        Assert.assertTrue(docs.contains(id));
        counter++;
      }

      Assert.assertEquals(counter, docs.size());
      assertEmbedded(ridBag.isEmbedded());
    }

    document.save();
    db.commit();

    Assert.assertEquals(ridBag.size(), 0);
    document.reload();

    ridBag = document.field("ridBag");
    Assert.assertEquals(ridBag.size(), 0);
    Assert.assertEquals(docs.size(), 0);
  }

  public void testAddMixedValues() {
    ORidBag ridBag = new ORidBag();
    ODocument document = new ODocument();
    document.field("ridBag", ridBag);
    assertEmbedded(ridBag.isEmbedded());

    List<OIdentifiable> itemsToAdd = new ArrayList<OIdentifiable>();

    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save();

      for (int k = 0; k < 2; k++) {
        ridBag.add(docToAdd);
        itemsToAdd.add(docToAdd);
      }
    }

    assertEmbedded(ridBag.isEmbedded());
    document.save();

    document.reload();
    ridBag = document.field("ridBag");

    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save();

      for (int k = 0; k < 2; k++) {
        ridBag.add(docToAdd);
        itemsToAdd.add(docToAdd);
      }
    }

    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      ridBag.add(docToAdd);
      itemsToAdd.add(docToAdd);
    }

    assertEmbedded(ridBag.isEmbedded());
    document.save();

    document.reload();
    ridBag = document.field("ridBag");

    db.begin();

    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save();

      for (int k = 0; k < 2; k++) {
        ridBag.add(docToAdd);
        itemsToAdd.add(docToAdd);
      }
    }
    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      ridBag.add(docToAdd);
      itemsToAdd.add(docToAdd);
    }

    assertEmbedded(ridBag.isEmbedded());
    document.save();

    db.commit();
    assertEmbedded(ridBag.isEmbedded());

    Assert.assertEquals(ridBag.size(), itemsToAdd.size());
    document.reload();
    Assert.assertEquals(ridBag.size(), itemsToAdd.size());

    for (OIdentifiable id : ridBag)
      Assert.assertTrue(itemsToAdd.remove(id));

    Assert.assertTrue(itemsToAdd.isEmpty());
  }

  public void testFromEmbeddedToSBTreeAndBack() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(7);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(4);

    ORidBag ridBag = new ORidBag();
    ODocument document = new ODocument();
    document.field("ridBag", ridBag);

    Assert.assertTrue(ridBag.isEmbedded());
    document.save();
    document.reload();

    ridBag = document.field("ridBag");
    Assert.assertTrue(ridBag.isEmbedded());

    List<OIdentifiable> addedItems = new ArrayList<OIdentifiable>();

    for (int i = 0; i < 6; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save();

      ridBag.add(docToAdd);
      addedItems.add(docToAdd);
    }

    document.save();

    document.reload();

    ridBag = document.field("ridBag");
    Assert.assertTrue(ridBag.isEmbedded());

    ODocument docToAdd = new ODocument();
    ridBag.add(docToAdd);
    addedItems.add(docToAdd);

    document.save();

    Assert.assertTrue(!ridBag.isEmbedded());

    List<OIdentifiable> addedItemsCopy = new ArrayList<OIdentifiable>(addedItems);
    for (OIdentifiable id : ridBag)
      Assert.assertTrue(addedItems.remove(id));

    Assert.assertTrue(addedItems.isEmpty());

    document.reload();

    ridBag = document.field("ridBag");
    Assert.assertTrue(!ridBag.isEmbedded());

    addedItems.addAll(addedItemsCopy);
    for (OIdentifiable id : ridBag)
      Assert.assertTrue(addedItems.remove(id));

    Assert.assertTrue(addedItems.isEmpty());

    addedItems.addAll(addedItemsCopy);

    for (int i = 0; i < 3; i++)
      ridBag.remove(addedItems.remove(i));

    addedItemsCopy.clear();
    addedItemsCopy.addAll(addedItems);

    document.save();

    Assert.assertTrue(ridBag.isEmbedded());

    for (OIdentifiable id : ridBag)
      Assert.assertTrue(addedItems.remove(id));

    Assert.assertTrue(addedItems.isEmpty());

    document.reload();

    ridBag = document.field("ridBag");
    Assert.assertTrue(ridBag.isEmbedded());

    addedItems.addAll(addedItemsCopy);
    for (OIdentifiable id : ridBag)
      Assert.assertTrue(addedItems.remove(id));

    Assert.assertTrue(addedItems.isEmpty());
  }

  public void testRemoveSavedInCommit() {
    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    ORidBag ridBag = new ORidBag();
    ODocument document = new ODocument();
    document.field("ridBag", ridBag);

    for (int i = 0; i < 5; i++) {
      ODocument docToAdd = new ODocument();
      ridBag.add(docToAdd);
      docToAdd.save();

      docsToAdd.add(docToAdd);
    }

    document.save();
    assertEmbedded(ridBag.isEmbedded());

    document.reload();
    ridBag = document.field("ridBag");
    assertEmbedded(ridBag.isEmbedded());

    db.begin();

    for (int i = 0; i < 5; i++) {
      ODocument docToAdd = new ODocument();
      ridBag.add(docToAdd);

      docsToAdd.add(docToAdd);
    }

    for (int i = 5; i < 10; i++) {
      ODocument docToAdd = docsToAdd.get(i).getRecord();
      docToAdd.save();
    }

    Iterator<OIdentifiable> iterator = docsToAdd.listIterator(7);
    while (iterator.hasNext()) {
      OIdentifiable docToAdd = iterator.next();
      ridBag.remove(docToAdd);
      iterator.remove();
    }

    document.save();
    db.commit();

    assertEmbedded(ridBag.isEmbedded());

    List<OIdentifiable> docsToAddCopy = new ArrayList<OIdentifiable>(docsToAdd);
    for (OIdentifiable id : ridBag)
      Assert.assertTrue(docsToAdd.remove(id));

    Assert.assertTrue(docsToAdd.isEmpty());

    docsToAdd.addAll(docsToAddCopy);

    document.reload();
    ridBag = document.field("ridBag");

    for (OIdentifiable id : ridBag)
      Assert.assertTrue(docsToAdd.remove(id));

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  private void massiveInsertionIteration(Random rnd, List<OIdentifiable> rids, ORidBag bag) {
    Iterator<OIdentifiable> ridsIterator = rids.iterator();
    Iterator<OIdentifiable> bagIterator = bag.iterator();

    while (ridsIterator.hasNext()) {
      assertTrue(bagIterator.hasNext());

      OIdentifiable ridValue = ridsIterator.next();
      OIdentifiable bagValue = bagIterator.next();

      assertEquals(bagValue, ridValue);
    }

    for (int i = 0; i < 100; i++) {
      if (rnd.nextDouble() < 0.2 & rids.size() > 5) {
        final int index = rnd.nextInt(rids.size());
        final OIdentifiable rid = rids.remove(index);
        bag.remove(rid);
      } else {
        final int positionIndex = rnd.nextInt(300);
        final OClusterPosition position = OClusterPositionFactory.INSTANCE.valueOf(positionIndex);

        final ORecordId recordId = new ORecordId(1, position);
        rids.add(recordId);
        bag.add(recordId);
      }
    }

    Collections.sort(rids);
    ridsIterator = rids.iterator();
    bagIterator = bag.iterator();

    while (ridsIterator.hasNext()) {
      assertTrue(bagIterator.hasNext());

      OIdentifiable ridValue = ridsIterator.next();
      OIdentifiable bagValue = bagIterator.next();

      assertEquals(bagValue, ridValue);

      if (rnd.nextDouble() < 0.05) {
        ridsIterator.remove();
        bagIterator.remove();
      }
    }

    ridsIterator = rids.iterator();
    bagIterator = bag.iterator();

    while (ridsIterator.hasNext()) {
      assertTrue(bagIterator.hasNext());

      OIdentifiable ridValue = ridsIterator.next();
      OIdentifiable bagValue = bagIterator.next();

      assertEquals(bagValue, ridValue);
    }
  }

  protected abstract void assertEmbedded(boolean isEmbedded);
}
