/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.*;

/**
 * @author Sergey Sitnikov
 */
public class VersionedDeleteTest {

  private ODatabaseDocumentTx db;

  @Before
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + VersionedDeleteTest.class.getSimpleName());

    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();
  }

  @After
  public void after() {
    db.drop();
  }

  @Test
  public void testDeleteRecycleNonInTx() {
    if (db.getStorage().isRemote())
      return;

    final ODocument doc = new ODocument().field("test", "test");
    doc.save(); // version is 1
    final ORID originalIdentity = doc.getIdentity().copy();

    db.delete(doc.getIdentity());

    doc.fromStream(new byte[] {}).field("resurrected", true);
    db.recycle(doc);

    ODocument doc2 = doc.getIdentity().getRecord();

    Assert.assertEquals(doc2.getIdentity(), originalIdentity);
    Assert.assertEquals(doc2.fields(), 1);
    Assert.assertEquals(doc2.field("resurrected"), true);
  }

  @Test
  public void testDeleteRecycleInTxCommit() {
    if (db.getStorage().isRemote())
      return;

    final ODocument doc = new ODocument().field("test", "test");
    doc.save(); // version is 1
    final ORID originalIdentity = doc.getIdentity();

    db.delete(doc.getIdentity());

    db.begin();
    doc.fromStream(new byte[] {}).field("resurrected", true);
    db.recycle(doc);
    db.commit();

    ODocument doc2 = doc.getIdentity().getRecord();

    Assert.assertEquals(originalIdentity, doc.getIdentity());
    Assert.assertEquals(doc2.fields(), 1);
    Assert.assertEquals(doc2.field("resurrected"), true);
  }

  @Test
  public void testDeleteRecycleInTxRollback() {
    if (db.getStorage().isRemote())
      return;

    final ODocument doc = new ODocument().field("test", "test");
    doc.save(); // version is 1

    db.delete(doc.getIdentity());

    db.begin();
    doc.fromStream(new byte[] {});
    db.recycle(doc);
    db.rollback();

    ODocument doc2 = doc.getIdentity().getRecord();
    Assert.assertNull(doc2);
  }

  @Test(expected = OConcurrentModificationException.class)
  public void testDeleteFutureVersion() {
    final ODocument doc = new ODocument();
    doc.save(); // version is 1

    db.delete(doc.getIdentity(), 2);
  }

  @Test(expected = OConcurrentModificationException.class)
  public void testDeletePreviousVersion() {
    final ODocument doc = new ODocument();
    doc.save(); // version is 1

    doc.field("key", "value").save(); // version is 2

    db.delete(doc.getIdentity(), 1);
  }

  @Ignore // tx version support must be reworked to handle this
  @Test
  public void testDeleteFutureVersionTx() {
    db.begin();
    final ODocument doc = new ODocument();
    doc.save(); // version is 1

    db.delete(doc.getIdentity(), 2);
    db.commit();
  }

  @Ignore // tx version support must be reworked to handle this
  @Test
  public void testDeletePreviousVersionTx() {
    db.begin();
    final ODocument doc = new ODocument();
    doc.save(); // version is 1

    doc.field("key", "value").save(); // version is 2

    db.delete(doc.getIdentity(), 1);
    db.commit();
  }

}
