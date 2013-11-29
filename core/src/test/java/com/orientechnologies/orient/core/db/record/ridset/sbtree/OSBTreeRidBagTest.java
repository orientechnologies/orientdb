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

import static org.testng.Assert.*;

import java.util.*;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeRidBagTest {

  @BeforeClass
  public void setUp() throws Exception {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/testdb/OSBTreeRidBagTest");
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();
    ODatabaseRecordThreadLocal.INSTANCE.set(db);
  }

  @AfterClass
  public void tearDown() throws Exception {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
    db.drop();
  }

  @Test
  public void testAdd() throws Exception {
    OSBTreeRidBag bag = new OSBTreeRidBag();

    bag.add(new ORecordId("#77:1"));
    Iterator<OIdentifiable> iterator = bag.iterator();
    Assert.assertTrue(iterator.hasNext());

    OIdentifiable identifiable = iterator.next();
    Assert.assertEquals(identifiable, new ORecordId("#77:1"));

    Assert.assertTrue(!iterator.hasNext());
  }

  @Test
  public void testAdd2() throws Exception {
    OSBTreeRidBag bag = new OSBTreeRidBag();

    bag.add(new ORecordId("#77:2"));
    bag.add(new ORecordId("#77:2"));
    assertEquals(bag.size(), 2);
  }

  @Test
  public void testEmptyIterator() throws Exception {
    OSBTreeRidBag ridSet = new OSBTreeRidBag();
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

    OSBTreeRidBag bag = new OSBTreeRidBag();
    bag.addAll(expected);

    assertEquals(bag.size(), 5);

    Set<OIdentifiable> actual = new HashSet<OIdentifiable>(8);
    for (OIdentifiable id : bag) {
      actual.add(id);
    }

    assertEquals(actual, expected);
  }

  @Test
  void testClear() {
    Set<OIdentifiable> initialValues = new HashSet<OIdentifiable>(8);

    initialValues.add(new ORecordId("#77:12"));
    initialValues.add(new ORecordId("#77:13"));
    initialValues.add(new ORecordId("#77:14"));
    initialValues.add(new ORecordId("#77:15"));
    initialValues.add(new ORecordId("#77:16"));

    final OSBTreeRidBag set = new OSBTreeRidBag();
    set.addAll(initialValues);

    set.clear();

    assertTrue(set.isEmpty());

    for (OIdentifiable o : set) {
      fail();
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

    final OSBTreeRidBag bag = new OSBTreeRidBag();
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
}
