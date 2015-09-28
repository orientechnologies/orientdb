package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public abstract class ORidBagTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public ORidBagTest(@org.testng.annotations.Optional String url) {
    super(url);
  }

  public void testAdd() throws Exception {
    ORidBag bag = new ORidBag();
    bag.setAutoConvertToRecord(false);

    bag.add(new ORecordId("#77:1"));
    Assert.assertTrue(bag.contains(new ORecordId("#77:1")));
    Assert.assertTrue(!bag.contains(new ORecordId("#78:2")));

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

    Assert.assertTrue(bag.contains(new ORecordId("#77:2")));
    Assert.assertTrue(!bag.contains(new ORecordId("#77:3")));

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

    Assert.assertTrue(bag.contains(new ORecordId("#77:3")));
    Assert.assertTrue(bag.contains(new ORecordId("#77:4")));
    Assert.assertTrue(bag.contains(new ORecordId("#77:5")));

    Assert.assertTrue(!bag.contains(new ORecordId("#77:2")));
    Assert.assertTrue(!bag.contains(new ORecordId("#77:6")));
    Assert.assertTrue(!bag.contains(new ORecordId("#77:1")));
    Assert.assertTrue(!bag.contains(new ORecordId("#77:0")));

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

    doc = database.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    Assert.assertTrue(bag.contains(new ORecordId("#77:3")));
    Assert.assertTrue(bag.contains(new ORecordId("#77:4")));
    Assert.assertTrue(bag.contains(new ORecordId("#77:5")));

    Assert.assertTrue(!bag.contains(new ORecordId("#77:2")));
    Assert.assertTrue(!bag.contains(new ORecordId("#77:6")));
    Assert.assertTrue(!bag.contains(new ORecordId("#77:1")));
    Assert.assertTrue(!bag.contains(new ORecordId("#77:0")));

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

    OStorage storage = database.getStorage();
    database.close();
    storage.close(true, false);

    database = new ODatabaseDocumentTx(database.getURL());
    database.open("admin", "admin");

    doc = database.load(rid);
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

    doc = database.load(rid);
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

    OStorage storage = database.getStorage();
    database.close();
    storage.close(true, false);

    database.activateOnCurrentThread();
    database.resetInitialization();
    database.open("admin", "admin");

    doc = database.load(rid);
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

    doc = database.load(rid);
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

    OStorage storage = database.getStorage();
    database.close();
    storage.close(true, false);

    database = new ODatabaseDocumentTx(database.getURL());
    database.open("admin", "admin");

    doc = database.load(rid);
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

    doc = database.load(rid);
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

    OStorage storage = database.getStorage();
    database.close();
    storage.close(true, false);

    database = new ODatabaseDocumentTx(database.getURL());
    database.open("admin", "admin");

    doc = database.load(rid);
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

    doc = database.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());
  }

  public void testCycle() {
    ODocument docOne = new ODocument();
    ORidBag ridBagOne = new ORidBag();

    ODocument docTwo = new ODocument();
    ORidBag ridBagTwo = new ORidBag();

    docOne.field("ridBag", ridBagOne);
    docTwo.field("ridBag", ridBagTwo);

    ridBagOne.add(docTwo);
    ridBagTwo.add(docOne);

    docOne.save();

    docOne = database.load(docOne.getIdentity(), "*:-1", false);
    ridBagOne = docOne.field("ridBag");

    docTwo = database.load(docTwo.getIdentity(), "*:-1", false);
    ridBagTwo = docTwo.field("ridBag");

    Assert.assertEquals(ridBagOne.iterator().next(), docTwo);
    Assert.assertEquals(ridBagTwo.iterator().next(), docOne);
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

    OStorage storage = database.getStorage();
    database.close();
    storage.close(true, false);

    database = new ODatabaseDocumentTx(database.getURL());
    database.open("admin", "admin");

    doc = database.load(rid);
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

    doc = database.load(rid);
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

    OStorage storage = database.getStorage();
    database.close();
    storage.close(true, false);

    database = new ODatabaseDocumentTx(database.getURL());
    database.open("admin", "admin");

    doc = database.load(id);
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
      document = database.load(rid);
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

    database.begin();
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
    database.commit();

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

    database.begin();

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

    database.commit();
    assertEmbedded(ridBag.isEmbedded());

    Assert.assertEquals(ridBag.size(), itemsToAdd.size());
    document.reload();
    Assert.assertEquals(ridBag.size(), itemsToAdd.size());

    for (OIdentifiable id : ridBag)
      Assert.assertTrue(itemsToAdd.remove(id));

    Assert.assertTrue(itemsToAdd.isEmpty());
  }

  public void testFromEmbeddedToSBTreeAndBack() throws IOException {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(7);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(4);

    if (database.getStorage() instanceof OStorageProxy) {
      OServerAdmin server = new OServerAdmin(database.getURL()).connect("root", ODatabaseHelper.getServerRootPassword());
      server.setGlobalConfiguration(OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, 7);
      server.setGlobalConfiguration(OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, 4);
    }

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

    database.begin();

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
    database.commit();

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

  @Test
  public void testSizeNotChangeAfterRemoveNotExistentElement() throws Exception {
    final ODocument bob = new ODocument();
    final ODocument fred = new ODocument().save();
    final ODocument jim = new ODocument().save();

    ORidBag teamMates = new ORidBag();

    teamMates.add(bob);
    teamMates.add(fred);

    Assert.assertEquals(teamMates.size(), 2);

    teamMates.remove(jim);

    Assert.assertEquals(teamMates.size(), 2);
  }

  @Test
  public void testRemoveNotExistentElementAndAddIt() throws Exception {
    ORidBag teamMates = new ORidBag();

    final ODocument bob = new ODocument().save();

    teamMates.remove(bob);

    Assert.assertEquals(teamMates.size(), 0);

    teamMates.add(bob);

    Assert.assertEquals(teamMates.size(), 1);
    Assert.assertEquals(teamMates.iterator().next().getIdentity(), bob.getIdentity());
  }

  public void testDocumentHelper() {
    ODocument document = new ODocument();
    ODocument embeddedDocument = new ODocument();
    List<ODocument> embeddedList = new ArrayList<ODocument>();

    ORidBag highLevelRidBag = new ORidBag();
    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      for (int j = 0; j < 2; j++)
        highLevelRidBag.add(docToAdd);
    }

    ORidBag embeddedRidBag = new ORidBag();
    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      embeddedRidBag.add(docToAdd);
    }

    document.field("ridBag", highLevelRidBag);
    embeddedList.add(embeddedDocument);
    embeddedDocument.field("ridBag", embeddedRidBag);
    document.field("embeddedList", embeddedList, OType.EMBEDDEDLIST);

    document.save();

    document.reload();

    ODocument documentCopy = database.load(document.getIdentity(), "*:-1", true);
    Assert.assertNotSame(document, documentCopy);
    Assert.assertTrue(ODocumentHelper.hasSameContentOf(document, database, documentCopy, database, null));

    Iterator<OIdentifiable> iterator = documentCopy.<ORidBag> field("ridBag").iterator();
    iterator.next();
    iterator.remove();

    Assert.assertTrue(!ODocumentHelper.hasSameContentOf(document, database, documentCopy, database, null));
    documentCopy.reload("*:-1", true);

    embeddedList = documentCopy.field("embeddedList");
    ODocument doc = embeddedList.get(0);

    iterator = doc.<ORidBag> field("ridBag").iterator();
    iterator.next();
    iterator.remove();

    Assert.assertTrue(!ODocumentHelper.hasSameContentOf(document, database, documentCopy, database, null));

    documentCopy.reload("*:-1", true);
    ODocument docToAdd = new ODocument();
    docToAdd.save();

    iterator = documentCopy.<ORidBag> field("ridBag").iterator();
    iterator.next();
    iterator.remove();

    documentCopy.<ORidBag> field("ridBag").add(docToAdd.getIdentity());
    Assert.assertTrue(!ODocumentHelper.hasSameContentOf(document, database, documentCopy, database, null));

    documentCopy.reload("*:-1", true);
    embeddedList = documentCopy.field("embeddedList");
    doc = embeddedList.get(0);

    iterator = doc.<ORidBag> field("ridBag").iterator();
    OIdentifiable remvedItem = iterator.next();
    iterator.remove();
    doc.<ORidBag> field("ridBag").add(docToAdd.getIdentity());

    Assert.assertTrue(!ODocumentHelper.hasSameContentOf(document, database, documentCopy, database, null));
    doc.<ORidBag> field("ridBag").remove(docToAdd.getIdentity());
    doc.<ORidBag> field("ridBag").add(remvedItem);

    Assert.assertTrue(ODocumentHelper.hasSameContentOf(document, database, documentCopy, database, null));
  }

  public void testAddNewItemsAndRemoveThem() {
    final List<OIdentifiable> rids = new ArrayList<OIdentifiable>();
    ORidBag ridBag = new ORidBag();
    int size = 0;
    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();

      for (int k = 0; k < 2; k++) {
        ridBag.add(docToAdd);
        rids.add(docToAdd);
        size++;
      }
    }

    Assert.assertEquals(ridBag.size(), size);
    ODocument document = new ODocument();
    document.field("ridBag", ridBag);
    document.save();

    document = database.load(document.getIdentity(), "*:-1", true);
    ridBag = document.field("ridBag");
    Assert.assertEquals(ridBag.size(), size);

    final List<OIdentifiable> newDocs = new ArrayList<OIdentifiable>();
    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();

      for (int k = 0; k < 2; k++) {
        ridBag.add(docToAdd);
        rids.add(docToAdd);
        newDocs.add(docToAdd);
        size++;
      }
    }

    Assert.assertEquals(ridBag.size(), size);

    Random rnd = new Random();

    for (int i = 0; i < newDocs.size(); i++) {
      if (rnd.nextBoolean()) {
        OIdentifiable newDoc = newDocs.get(i);
        rids.remove(newDoc);
        ridBag.remove(newDoc);
        newDocs.remove(newDoc);

        size--;
      }
    }

    for (OIdentifiable identifiable : ridBag) {
      if (newDocs.contains(identifiable) && rnd.nextBoolean()) {
        ridBag.remove(identifiable);
        rids.remove(identifiable);

        size--;
      }
    }

    Assert.assertEquals(ridBag.size(), size);
    List<OIdentifiable> ridsCopy = new ArrayList<OIdentifiable>(rids);

    for (OIdentifiable identifiable : ridBag) {
      Assert.assertTrue(rids.remove(identifiable));
    }

    Assert.assertTrue(rids.isEmpty());

    document.save();

    document = database.load(document.getIdentity(), "*:-1", false);
    ridBag = document.field("ridBag");

    rids.addAll(ridsCopy);
    for (OIdentifiable identifiable : ridBag) {
      Assert.assertTrue(rids.remove(identifiable));
    }

    Assert.assertTrue(rids.isEmpty());
    Assert.assertEquals(ridBag.size(), size);
  }

  public void testJsonSerialization() {
    ODocument externalDoc = new ODocument();
    ODocument testDocument = new ODocument();
    ORidBag highLevelRidBag = new ORidBag();
    for (int i = 0; i < 10; i++)
      highLevelRidBag.add(new ODocument());

    testDocument.field("ridBag", highLevelRidBag);
    testDocument.field("externalDoc", externalDoc);

    final List<ODocument> embeddedList = new ArrayList<ODocument>();
    ODocument embeddedListDoc = new ODocument();
    ORidBag embeddedListDocRidBag = new ORidBag();
    for (int i = 0; i < 10; i++)
      embeddedListDocRidBag.add(new ODocument());

    embeddedListDoc.field("ridBag", embeddedListDocRidBag);
    embeddedListDoc.field("externalDoc", externalDoc);
    embeddedList.add(embeddedListDoc);

    Set<ODocument> embeddedSet = new HashSet<ODocument>();
    ODocument embeddedSetDoc = new ODocument();
    ORidBag embeddedSetDocRidBag = new ORidBag();
    for (int i = 0; i < 10; i++)
      embeddedSetDocRidBag.add(new ODocument());

    embeddedSetDoc.field("ridBag", embeddedSetDocRidBag);
    embeddedSetDoc.field("externalDoc", externalDoc);
    embeddedSet.add(embeddedSetDoc);

    Map<String, ODocument> embeddedMap = new HashMap<String, ODocument>();
    ODocument embeddedMapDoc = new ODocument();
    ORidBag embeddedMapDocRidBag = new ORidBag();
    for (int i = 0; i < 10; i++)
      embeddedMapDocRidBag.add(new ODocument());
    embeddedMapDoc.field("ridBag", embeddedMapDocRidBag);
    embeddedMapDoc.field("externalDoc", externalDoc);
    embeddedMap.put("k1", embeddedMapDoc);

    testDocument.field("embeddedList", embeddedList, OType.EMBEDDEDLIST);
    testDocument.field("embeddedSet", embeddedSet, OType.EMBEDDEDSET);
    testDocument.field("embeddedMap", embeddedMap, OType.EMBEDDEDMAP);

    testDocument.save();
    testDocument.reload();

    final String json = testDocument.toJSON();

    ODocument doc = new ODocument();
    doc.fromJSON(json);

    Assert.assertTrue(ODocumentHelper.hasSameContentOf(doc, database, testDocument, database, null));
  }

  public void stackOverflowDuringToString() {
    final OrientGraph graph = new OrientGraph(database);

    OrientVertex a = graph.addVertex("A");
    OrientVertex b = graph.addVertex("B");
    OrientVertex c = graph.addVertex("C");

    a.addEdge("link", b);
    a.addEdge("link", c);
    b.addEdge("link", a);
    b.addEdge("link", c);
    c.addEdge("link", a);
    c.addEdge("link", b);

    // System.out.println("A: " + a.getRecord());
    // System.out.println("B: " + b.getRecord());
    // System.out.println("C: " + c.getRecord());

    database.commit();
  }

  protected abstract void assertEmbedded(boolean isEmbedded);

  private void massiveInsertionIteration(Random rnd, List<OIdentifiable> rids, ORidBag bag) {
    Iterator<OIdentifiable> bagIterator = bag.iterator();

    while (bagIterator.hasNext()) {
      OIdentifiable bagValue = bagIterator.next();
      Assert.assertTrue(rids.contains(bagValue));
    }

    Assert.assertEquals(bag.size(), rids.size());

    for (int i = 0; i < 100; i++) {
      if (rnd.nextDouble() < 0.2 & rids.size() > 5) {
        final int index = rnd.nextInt(rids.size());
        final OIdentifiable rid = rids.remove(index);
        bag.remove(rid);
      } else {
        final int positionIndex = rnd.nextInt(300);
        final long position = positionIndex;

        final ORecordId recordId = new ORecordId(1, position);
        rids.add(recordId);
        bag.add(recordId);
      }
    }

    bagIterator = bag.iterator();

    while (bagIterator.hasNext()) {
      final OIdentifiable bagValue = bagIterator.next();
      Assert.assertTrue(rids.contains(bagValue));

      if (rnd.nextDouble() < 0.05) {
        bagIterator.remove();
        Assert.assertTrue(rids.remove(bagValue));
      }
    }

    Assert.assertEquals(bag.size(), rids.size());
    bagIterator = bag.iterator();

    while (bagIterator.hasNext()) {
      final OIdentifiable bagValue = bagIterator.next();
      Assert.assertTrue(rids.contains(bagValue));
    }
  }
}
