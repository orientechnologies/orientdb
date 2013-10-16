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

package com.orientechnologies.orient.core.db.record.ridset.sbtree;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeRIDSetTest {

  private ODocument doc;

  @BeforeClass
  public void setUp() throws Exception {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/testdb/OSBTreeRIDSetTest");
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();
    ODatabaseRecordThreadLocal.INSTANCE.set(db);
    doc = new ODocument();
    doc.save();
  }

  @AfterClass
  public void tearDown() throws Exception {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
    db.drop();
  }

  @Test
  public void testInitialization() throws Exception {
    OSBTreeRIDSet set = new OSBTreeRIDSet(doc);

    assertNotNull(set.getFileName());
    assertNotNull(set.getRootPointer());
    assertTrue(set.isEmpty());
  }

  @Test
  public void testAdd() throws Exception {
    OSBTreeRIDSet set = new OSBTreeRIDSet(doc);

    boolean result = set.add(new ORecordId("#77:1"));
    assertTrue(result);
    assertEquals(set.size(), 1);
    assertFalse(set.isEmpty());
  }

  @Test
  public void testAdd2() throws Exception {
    OSBTreeRIDSet set = new OSBTreeRIDSet(doc);

    boolean result = set.add(new ORecordId("#77:2"));
    assertTrue(result);
    assertEquals(set.size(), 1);

    result = set.add(new ORecordId("#77:2"));
    assertFalse(result);
    assertEquals(set.size(), 1);
  }

  @Test
  public void testEmptyIterator() throws Exception {
    OSBTreeRIDSet ridSet = new OSBTreeRIDSet(doc);
    assertEquals(ridSet.size(), 0);

    for (OIdentifiable id : ridSet) {
      Assert.fail();
    }
  }

  @Test
  public void testAddAllAndIterator() throws Exception {
    Set<OIdentifiable> expected = new HashSet<OIdentifiable>(8);

    expected.add(new ORecordId("#77:12"));
    expected.add(new ORecordId("#77:13"));
    expected.add(new ORecordId("#77:14"));
    expected.add(new ORecordId("#77:15"));
    expected.add(new ORecordId("#77:16"));

    OSBTreeRIDSet ridSet = new OSBTreeRIDSet(doc);
    ridSet.setAutoConvertToRecord(false);
    ridSet.addAll(expected);
    assertEquals(ridSet.size(), 5);

    Set<OIdentifiable> actual = new HashSet<OIdentifiable>(8);
    for (OIdentifiable id : ridSet) {
      actual.add(id);
    }

    assertEquals(actual, expected);
  }

  @Test
  public void testContains() {
    final OSBTreeRIDSet set = new OSBTreeRIDSet(doc);

    final ORecordId id1 = new ORecordId("#77:1");
    set.add(id1);

    final ORecordId id2 = new ORecordId("#77:2");

    assertTrue(set.contains(id1));
    assertTrue(set.contains(new ORecordId("#77:1")));
    assertFalse(set.contains(id2));
  }

  @Test
  public void testContainsAll() {
    final OSBTreeRIDSet set = new OSBTreeRIDSet(doc);

    set.addAll(Arrays.asList(new ORecordId("#78:1"), new ORecordId("#78:2"), new ORecordId("#78:3")));

    assertTrue(set.containsAll(Arrays.asList(new ORecordId("#78:1"), new ORecordId("#78:2"), new ORecordId("#78:3"))));
    assertFalse(set.containsAll(Arrays.asList(new ORecordId("#78:5"), new ORecordId("#78:2"), new ORecordId("#78:3"))));
    assertFalse(set.containsAll(Arrays.asList(new ORecordId("#78:5"), new ORecordId("#78:6"), new ORecordId("#78:7"))));
  }

  @Test
  void testClear() {
    Set<OIdentifiable> initialValues = new HashSet<OIdentifiable>(8);

    initialValues.add(new ORecordId("#77:12"));
    initialValues.add(new ORecordId("#77:13"));
    initialValues.add(new ORecordId("#77:14"));
    initialValues.add(new ORecordId("#77:15"));
    initialValues.add(new ORecordId("#77:16"));

    final OSBTreeRIDSet set = new OSBTreeRIDSet(doc);
    set.addAll(initialValues);

    set.clear();

    assertTrue(set.isEmpty());

    for (OIdentifiable o : set) {
      fail();
    }

    for (OIdentifiable initialValue : initialValues) {
      assertFalse(set.contains(initialValue));
    }
  }

  @Test
  public void testRemove() {
    final Set<OIdentifiable> expected = new HashSet<OIdentifiable>(8);

    expected.add(new ORecordId("#77:12"));
    expected.add(new ORecordId("#77:13"));
    expected.add(new ORecordId("#77:14"));
    expected.add(new ORecordId("#77:15"));
    expected.add(new ORecordId("#77:16"));

    final OSBTreeRIDSet set = new OSBTreeRIDSet(doc);
    set.addAll(expected);

    assertFalse(set.remove(new ORecordId("#77:23")));
    assertTrue(set.containsAll(expected));

    expected.remove(new ORecordId("#77:14"));
    assertTrue(set.remove(new ORecordId("#77:14")));
    assertFalse(set.contains(new ORecordId("#77:14")));
    assertTrue(set.containsAll(expected));
  }

  @Test
  public void testRemoveAll() {
    final Set<OIdentifiable> expected = new HashSet<OIdentifiable>(8);

    expected.add(new ORecordId("#77:12"));
    expected.add(new ORecordId("#77:13"));
    expected.add(new ORecordId("#77:14"));
    expected.add(new ORecordId("#77:15"));
    expected.add(new ORecordId("#77:16"));

    final OSBTreeRIDSet set = new OSBTreeRIDSet(doc);
    set.addAll(expected);

    assertFalse(set.removeAll(Arrays.asList(new ORecordId("#77:23"), new ORecordId("#77:27"))));
    assertTrue(set.containsAll(expected));

    final List<ORecordId> removedRecords = Arrays.asList(new ORecordId("#77:14"), new ORecordId("#77:15"));
    expected.removeAll(removedRecords);
    assertTrue(set.removeAll(removedRecords));
    for (ORecordId removedRecord : removedRecords) {
      assertFalse(set.contains(removedRecord));
    }
    assertTrue(set.containsAll(expected));
  }

  @Test
  public void testRetainAll() {
    final Set<OIdentifiable> initialRecords = new HashSet<OIdentifiable>(8);

    initialRecords.add(new ORecordId("#77:12"));
    initialRecords.add(new ORecordId("#77:13"));
    initialRecords.add(new ORecordId("#77:14"));
    initialRecords.add(new ORecordId("#77:15"));
    initialRecords.add(new ORecordId("#77:16"));

    final OSBTreeRIDSet set = new OSBTreeRIDSet(doc);
    set.setAutoConvertToRecord(false);
    set.addAll(initialRecords);

    assertFalse(set.retainAll(initialRecords));

    final List<ORecordId> expected = Arrays.asList(new ORecordId("#77:14"), new ORecordId("#77:15"));
    final List<ORecordId> records = new ArrayList<ORecordId>();
    records.addAll(Arrays.asList(new ORecordId("#77:24"), new ORecordId("#77:25")));
    records.addAll(expected);

    assertTrue(set.retainAll(records));

    assertTrue(set.containsAll(expected));
    for (ORecordId e : expected) {
      assertTrue(set.contains(e));
    }
  }
}
