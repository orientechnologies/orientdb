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

package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * @author Sergey Sitnikov
 */
public class NonDurableTxTest {

  private ODatabaseDocumentTx       db;
  private OAtomicOperationsManager  atomicOperationsManager;
  private OWriteAheadLog            wal;

  @Before
  public void before() {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    db = new ODatabaseDocumentTx("plocal:" + buildDirectory + File.separator + NonDurableTxTest.class.getSimpleName());

    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();

    final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) db.getStorage();
    atomicOperationsManager = storage.getAtomicOperationsManager();
    wal = storage.getWALInstance();
  }

  @After
  public void after() {
    db.drop();
  }

  @Test
  public void testChangesStored() {
    db.begin();
    db.getTransaction().setUsingLog(false);
    final ODocument doc1 = db.newInstance().field("tx-key", "tx-value").save();
    db.commit();

    doc1.field("non-tx-key", "non-tx-value").save();

    db.close();
    db.open("admin", "admin");
    final ODocument doc2 = db.load(doc1.getIdentity());
    Assert.assertEquals("tx-value", doc2.field("tx-key"));
    Assert.assertEquals("non-tx-value", doc2.field("non-tx-key"));
  }

  @Test
  public void testChangesStoredWhileWalDisabledInConfiguration() {
    OGlobalConfiguration.USE_WAL.setValue(false);
    before(); // reopen DB with new configuration settings

    try {
      db.begin();
      db.getTransaction().setUsingLog(false);
      final ODocument doc1 = db.newInstance().field("tx-key", "tx-value").save();
      db.commit();

      doc1.field("non-tx-key", "non-tx-value").save();

      db.close();
      db.open("admin", "admin");
      final ODocument doc2 = db.load(doc1.getIdentity());
      Assert.assertEquals("tx-value", doc2.field("tx-key"));
      Assert.assertEquals("non-tx-value", doc2.field("non-tx-key"));
    } finally {
      OGlobalConfiguration.USE_WAL.setValue(true);
    }
  }

  @Test
  public void testWalNotGrowingWhileWalDisabledInTx() throws Exception {
    db.newInstance().field("some-unrelated-key", "some-unrelated-value").save();

    wal.flush();
    final OLogSequenceNumber startLsn = wal.getFlushedLsn();

    db.begin();
    db.getTransaction().setUsingLog(false);
    db.newInstance().field("tx-key", "tx-value").save();
    db.commit();

    wal.flush();
    final OLogSequenceNumber endLsn = wal.getFlushedLsn();

    Assert.assertEquals(startLsn, endLsn);
  }

  @Test
  public void testWalNotGrowingWhileWalDisabledInAtomicManager() throws Exception {
    db.newInstance().field("some-unrelated-key", "some-unrelated-value").save();

    wal.flush();
    final OLogSequenceNumber startLsn = wal.getFlushedLsn();

    atomicOperationsManager.switchOnUnsafeMode();
    try {
      db.begin();
      final ODocument doc1 = db.newInstance().field("tx-key", "tx-value").save();
      db.commit();

      doc1.field("non-tx-key", "non-tx-value").save();

      wal.flush();
      final OLogSequenceNumber endLsn = wal.getFlushedLsn();

      Assert.assertEquals(startLsn, endLsn);
    } finally {
      atomicOperationsManager.switchOffUnsafeMode();
    }
  }

}
