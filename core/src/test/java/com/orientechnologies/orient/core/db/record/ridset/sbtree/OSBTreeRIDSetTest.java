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

import java.util.HashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeRIDSetTest {
  @BeforeMethod
  public void setUp() throws Exception {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/testdb/OSBTreeRIDSetTest");
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();
  }

  @Test
  public void testInitialization() throws Exception {
    OSBTreeRIDSet set = new OSBTreeRIDSet();

    Assert.assertNotNull(set.getFileName());
    Assert.assertNotNull(set.getRootIndex());
  }

  @Test
  public void testAdd() throws Exception {
    OSBTreeRIDSet set = new OSBTreeRIDSet();

    boolean result = set.add(new ORecordId("#77:1"));
    Assert.assertTrue(result);
    Assert.assertEquals(set.size(), 1);
  }

  @Test
  public void testAdd2() throws Exception {
    OSBTreeRIDSet set = new OSBTreeRIDSet();

    boolean result = set.add(new ORecordId("#77:2"));
    Assert.assertTrue(result);
    Assert.assertEquals(set.size(), 1);

    result = set.add(new ORecordId("#77:2"));
    Assert.assertFalse(result);
    Assert.assertEquals(set.size(), 1);
  }

  @Test
  public void testEmptyIterator() throws Exception {
    OSBTreeRIDSet ridSet = new OSBTreeRIDSet();
    Assert.assertEquals(ridSet.size(), 0);

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

    OSBTreeRIDSet ridSet = new OSBTreeRIDSet();
    ridSet.addAll(expected);
    Assert.assertEquals(ridSet.size(), 5);

    Set<OIdentifiable> actual = new HashSet<OIdentifiable>(8);
    for (OIdentifiable id : ridSet) {
      actual.add(id);
    }

    Assert.assertEquals(actual, expected);
  }
}
