/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
@Test
public class OSBTreeRidBagTest {

  private ODatabaseDocumentTx db;

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
    OSBTreeRidBag bag = new OSBTreeRidBag();
    bag.setAutoConvertToRecord(false);

    bag.add(new ORecordId("#77:1"));
    Iterator<OIdentifiable> iterator = bag.iterator();
    Assert.assertTrue(iterator.hasNext());

    OIdentifiable identifiable = iterator.next();
    Assert.assertEquals(identifiable, new ORecordId("#77:1"));

    Assert.assertTrue(!iterator.hasNext());
  }

  public void testAdd2() throws Exception {
    OSBTreeRidBag bag = new OSBTreeRidBag();
    bag.setAutoConvertToRecord(false);

    bag.add(new ORecordId("#77:2"));
    bag.add(new ORecordId("#77:2"));
    assertEquals(bag.size(), 2);
  }

  public void testAddRemove() {
    OSBTreeRidBag bag = new OSBTreeRidBag();
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

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());
  }

  public void testAddRemoveSBTreeContainsValues() {
    OSBTreeRidBag bag = new OSBTreeRidBag();
    bag.setAutoConvertToRecord(false);

    bag.add(new ORecordId("#77:2"));
    bag.add(new ORecordId("#77:2"));
    bag.add(new ORecordId("#77:3"));
    bag.add(new ORecordId("#77:4"));
    bag.add(new ORecordId("#77:4"));
    bag.add(new ORecordId("#77:4"));
    bag.add(new ORecordId("#77:5"));
    bag.add(new ORecordId("#77:6"));

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
    doc.field("ridbag", bag);
    doc.save();

    rid = doc.getIdentity();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());
  }

  public void testAddRemoveDuringIterationSBTreeContainsValues() {
    OSBTreeRidBag bag = new OSBTreeRidBag();
    bag.setAutoConvertToRecord(false);

    bag.add(new ORecordId("#77:2"));
    bag.add(new ORecordId("#77:2"));
    bag.add(new ORecordId("#77:3"));
    bag.add(new ORecordId("#77:4"));
    bag.add(new ORecordId("#77:4"));
    bag.add(new ORecordId("#77:4"));
    bag.add(new ORecordId("#77:5"));
    bag.add(new ORecordId("#77:6"));

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

    doc = new ODocument();
    doc.field("ridbag", bag);
    doc.save();

    rid = doc.getIdentity();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());
  }

  public void testEmptyIterator() throws Exception {
    OSBTreeRidBag bag = new OSBTreeRidBag();
    bag.setAutoConvertToRecord(false);
    assertEquals(bag.size(), 0);

    for (OIdentifiable id : bag) {
      Assert.fail();
    }
  }

  public void testAddRemoveNotExisting() {
    List<OIdentifiable> rids = new ArrayList<OIdentifiable>();

    OSBTreeRidBag bag = new OSBTreeRidBag();
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

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());

    for (OIdentifiable identifiable : bag)
      rids.add(identifiable);

    doc.save();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");

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

    OSBTreeRidBag bag = new OSBTreeRidBag();
    bag.setAutoConvertToRecord(false);

    bag.addAll(expected);

    assertEquals(bag.size(), 5);

    Set<OIdentifiable> actual = new HashSet<OIdentifiable>(8);
    for (OIdentifiable id : bag) {
      actual.add(id);
    }

    assertEquals(actual, expected);
  }

  public void testAddSBTreeAddInMemoryIterate() {
    List<OIdentifiable> rids = new ArrayList<OIdentifiable>();

    OSBTreeRidBag bag = new OSBTreeRidBag();
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

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());

    for (OIdentifiable identifiable : bag)
      rids.add(identifiable);

    doc = new ODocument();
    doc.field("ridbag", bag);
    doc.save();

    rid = doc.getIdentity();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");

    for (OIdentifiable identifiable : bag)
      assertTrue(rids.remove(identifiable));

    assertTrue(rids.isEmpty());
  }

  public void testAddSBTreeAddInMemoryIterateAndRemove() {
    List<OIdentifiable> rids = new ArrayList<OIdentifiable>();

    OSBTreeRidBag bag = new OSBTreeRidBag();
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

    bag.add(new ORecordId("#77:7"));
    rids.add(new ORecordId("#77:7"));

    bag.add(new ORecordId("#77:8"));
    rids.add(new ORecordId("#77:8"));

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
    doc.field("ridbag", bag);
    doc.save();

    rid = doc.getIdentity();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");

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

    final OSBTreeRidBag bag = new OSBTreeRidBag();
    bag.setAutoConvertToRecord(false);
    bag.addAll(expected);

    bag.remove(new ORecordId("#77:23"));

    final Set<OIdentifiable> expectedTwo = new HashSet<OIdentifiable>(8);
    expectedTwo.addAll(expected);

    for (OIdentifiable identifiable : bag) {
      assertTrue(expectedTwo.remove(identifiable));
    }

    Assert.assertTrue(expectedTwo.isEmpty());

    expected.remove(new ORecordId("#77:14"));
    bag.remove(new ORecordId("#77:14"));

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

    final OSBTreeRidBag ridSet = new OSBTreeRidBag();
    ridSet.addAll(expected);

    doc.field("ridbag", ridSet);
    doc.save();
    final ORID id = doc.getIdentity();

    OStorage storage = db.getStorage();
    db.close();
    storage.close(true);

    db.open("admin", "admin");

    doc = db.load(id);
    doc.setLazyLoad(false);

    final OSBTreeRidBag loaded = doc.field("ridbag");

    Assert.assertEquals(loaded.size(), expected.size());
    for (OIdentifiable identifiable : loaded)
      Assert.assertTrue(expected.remove(identifiable));

    Assert.assertTrue(expected.isEmpty());
  }

  @Test
  public void testSaveInBackOrder() throws Exception {
    ODocument docA = new ODocument().field("name", "A");
    ODocument docB = new ODocument().field("name", "B").save();

    OSBTreeRidBag ridBag = new OSBTreeRidBag();

    ridBag.add(docA);
    ridBag.add(docB);

    docA.save();
    ridBag.remove(docB);

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
    OSBTreeRidBag bag = new OSBTreeRidBag();
    Random random = new Random();
    List<OIdentifiable> rids = new ArrayList<OIdentifiable>();
    document.field("bag", bag);
    document.save();
    ORID rid = document.getIdentity();

    for (int i = 0; i < 10; i++) {
      document = db.load(rid);
      document.setLazyLoad(false);

      bag = document.field("bag");

      massiveInsertionIteration(random, rids, bag);

      document.save();
    }
    document.delete();
  }

  private void massiveInsertionIteration(Random rnd, List<OIdentifiable> rids, OSBTreeRidBag bag) {
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
}
