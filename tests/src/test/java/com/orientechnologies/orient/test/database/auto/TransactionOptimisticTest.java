/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.tx.ORollbackException;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "dictionary")
public class TransactionOptimisticTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public TransactionOptimisticTest(@Optional String iURL) {
    super(iURL);
  }

  @Test
  public void testTransactionOptimisticRollback() throws IOException {
    if (database.getClusterIdByName("binary") == -1) database.addBlobCluster("binary");

    long rec = database.countClusterElements("binary");

    database.begin();

    OBlob recordBytes = new ORecordBytes("This is the first version".getBytes());
    recordBytes.save("binary");

    database.rollback();

    Assert.assertEquals(database.countClusterElements("binary"), rec);
  }

  @Test(dependsOnMethods = "testTransactionOptimisticRollback")
  public void testTransactionOptimisticCommit() throws IOException {
    if (database.getClusterIdByName("binary") == -1) database.addBlobCluster("binary");

    long tot = database.countClusterElements("binary");

    database.begin();

    OBlob recordBytes = new ORecordBytes("This is the first version".getBytes());
    recordBytes.save("binary");

    database.commit();

    Assert.assertEquals(database.countClusterElements("binary"), tot + 1);
  }

  @Test(dependsOnMethods = "testTransactionOptimisticCommit")
  public void testTransactionOptimisticConcurrentException() throws IOException {
    if (database.getClusterIdByName("binary") == -1) database.addBlobCluster("binary");

    ODatabaseDocumentTx db2 = new ODatabaseDocumentTx(database.getURL());
    db2.open("admin", "admin");

    database.activateOnCurrentThread();
    OBlob record1 = new ORecordBytes("This is the first version".getBytes());
    record1.save("binary");

    try {
      database.begin();

      // RE-READ THE RECORD
      record1.load();

      ODatabaseRecordThreadLocal.instance().set(db2);
      OBlob record2 = db2.load(record1.getIdentity());

      record2.setDirty();
      record2.fromStream("This is the second version".getBytes());
      record2.save();

      ODatabaseRecordThreadLocal.instance().set(database);
      record1.setDirty();
      record1.fromStream("This is the third version".getBytes());
      record1.save();

      database.commit();

      Assert.assertTrue(false);

    } catch (OConcurrentModificationException e) {
      Assert.assertTrue(true);
      database.rollback();

    } finally {

      database.close();

      db2.activateOnCurrentThread();
      db2.close();
    }
  }

  @Test(dependsOnMethods = "testTransactionOptimisticConcurrentException")
  public void testTransactionOptimisticCacheMgmt1Db() throws IOException {
    if (database.getClusterIdByName("binary") == -1) database.addBlobCluster("binary");

    OBlob record = new ORecordBytes("This is the first version".getBytes());
    record.save();

    try {
      database.begin();

      // RE-READ THE RECORD
      record.load();
      int v1 = record.getVersion();
      record.setDirty();
      record.fromStream("This is the second version".getBytes());
      record.save();
      database.commit();

      record.reload();
      Assert.assertEquals(record.getVersion(), v1 + 1);
      Assert.assertTrue(new String(record.toStream()).contains("second"));
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "testTransactionOptimisticCacheMgmt1Db")
  public void testTransactionOptimisticCacheMgmt2Db() throws IOException {
    if (database.getClusterIdByName("binary") == -1) database.addBlobCluster("binary");

    ODatabaseDocumentTx db2 = new ODatabaseDocumentTx(database.getURL());
    db2.open("admin", "admin");

    OBlob record1 = new ORecordBytes("This is the first version".getBytes());
    record1.save();

    try {
      ODatabaseRecordThreadLocal.instance().set(database);
      database.begin();

      // RE-READ THE RECORD
      record1.load();
      int v1 = record1.getVersion();
      record1.setDirty();
      record1.fromStream("This is the second version".getBytes());
      record1.save();

      database.commit();

      db2.activateOnCurrentThread();

      OBlob record2 = db2.load(record1.getIdentity(), "*:-1", true);
      Assert.assertEquals(record2.getVersion(), v1 + 1);
      Assert.assertTrue(new String(record2.toStream()).contains("second"));

    } finally {

      database.activateOnCurrentThread();
      database.close();

      db2.activateOnCurrentThread();
      db2.close();
    }
  }

  @Test(dependsOnMethods = "testTransactionOptimisticCacheMgmt2Db")
  public void testTransactionMultipleRecords() throws IOException {
    final OSchema schema = database.getMetadata().getSchema();

    if (!schema.existsClass("Account")) schema.createClass("Account");

    long totalAccounts = database.countClusterElements("Account");

    String json =
        "{ \"@class\": \"Account\", \"type\": \"Residence\", \"street\": \"Piazza di Spagna\"}";

    database.begin(TXTYPE.OPTIMISTIC);
    for (int g = 0; g < 1000; g++) {
      ODocument doc = new ODocument("Account");
      doc.fromJSON(json);
      doc.field("nr", g);

      doc.save();
    }
    database.commit();

    Assert.assertEquals(database.countClusterElements("Account"), totalAccounts + 1000);

    database.close();
  }

  @SuppressWarnings("unchecked")
  public void createGraphInTx() {
    final OSchema schema = database.getMetadata().getSchema();

    if (!schema.existsClass("Profile")) schema.createClass("Profile");

    database.begin();

    ODocument kim = new ODocument("Profile").field("name", "Kim").field("surname", "Bauer");
    ODocument teri = new ODocument("Profile").field("name", "Teri").field("surname", "Bauer");
    ODocument jack = new ODocument("Profile").field("name", "Jack").field("surname", "Bauer");

    ((HashSet<ODocument>) jack.field("following", new HashSet<ODocument>()).field("following"))
        .add(kim);
    ((HashSet<ODocument>) kim.field("following", new HashSet<ODocument>()).field("following"))
        .add(teri);
    ((HashSet<ODocument>) teri.field("following", new HashSet<ODocument>()).field("following"))
        .add(jack);

    jack.save();

    database.commit();

    database.close();
    database.open("admin", "admin");

    ODocument loadedJack = database.load(jack.getIdentity());
    Assert.assertEquals(loadedJack.field("name"), "Jack");
    Collection<ODocument> jackFollowings = loadedJack.field("following");
    Assert.assertNotNull(jackFollowings);
    Assert.assertEquals(jackFollowings.size(), 1);

    ODocument loadedKim = jackFollowings.iterator().next();
    Assert.assertEquals(loadedKim.field("name"), "Kim");
    Collection<ODocument> kimFollowings = loadedKim.field("following");
    Assert.assertNotNull(kimFollowings);
    Assert.assertEquals(kimFollowings.size(), 1);

    ODocument loadedTeri = kimFollowings.iterator().next();
    Assert.assertEquals(loadedTeri.field("name"), "Teri");
    Collection<ODocument> teriFollowings = loadedTeri.field("following");
    Assert.assertNotNull(teriFollowings);
    Assert.assertEquals(teriFollowings.size(), 1);

    Assert.assertEquals(teriFollowings.iterator().next().field("name"), "Jack");

    database.close();
  }

  public void testNestedTx() throws Exception {
    final ExecutorService executorService = Executors.newSingleThreadExecutor();

    final Callable<Void> assertEmptyRecord =
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            final ODatabaseDocumentTx db = new ODatabaseDocumentTx(database.getURL());
            db.open("admin", "admin");
            try {
              Assert.assertEquals(db.countClass("NestedTxClass"), 0);
            } finally {
              db.close();
            }

            return null;
          }
        };

    final OSchema schema = database.getMetadata().getSchema();
    if (!schema.existsClass("NestedTxClass")) schema.createClass("NestedTxClass");

    database.begin();

    final ODocument externalDocOne = new ODocument("NestedTxClass");
    externalDocOne.field("v", "val1");
    externalDocOne.save();

    Future assertFuture = executorService.submit(assertEmptyRecord);
    assertFuture.get();

    database.begin();

    final ODocument externalDocTwo = new ODocument("NestedTxClass");
    externalDocTwo.field("v", "val2");
    externalDocTwo.save();

    assertFuture = executorService.submit(assertEmptyRecord);
    assertFuture.get();

    database.commit();

    assertFuture = executorService.submit(assertEmptyRecord);
    assertFuture.get();

    final ODocument externalDocThree = new ODocument("NestedTxClass");
    externalDocThree.field("v", "val3");
    externalDocThree.save();

    database.commit();

    Assert.assertTrue(!database.getTransaction().isActive());
    Assert.assertEquals(database.countClass("NestedTxClass"), 3);
  }

  public void testNestedTxRollbackOne() throws Exception {
    final ExecutorService executorService = Executors.newSingleThreadExecutor();

    final Callable<Void> assertEmptyRecord =
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            final ODatabaseDocumentTx db = new ODatabaseDocumentTx(database.getURL());
            db.open("admin", "admin");
            try {
              Assert.assertEquals(db.countClass("NestedTxRollbackOne"), 1);
            } finally {
              db.close();
            }

            return null;
          }
        };

    final OSchema schema = database.getMetadata().getSchema();
    if (!schema.existsClass("NestedTxRollbackOne")) schema.createClass("NestedTxRollbackOne");

    ODocument brokenDocOne = new ODocument("NestedTxRollbackOne");
    brokenDocOne.save();

    brokenDocOne = database.load(brokenDocOne.getIdentity(), "*:-1", true);

    ODocument brokenDocTwo = database.load(brokenDocOne.getIdentity(), "*:-1", true);
    brokenDocTwo.setDirty();
    brokenDocTwo.field("v", "vstr");
    brokenDocTwo.save();

    try {
      database.begin();

      final ODocument externalDocOne = new ODocument("NestedTxRollbackOne");
      externalDocOne.field("v", "val1");
      externalDocOne.save();

      Future assertFuture = executorService.submit(assertEmptyRecord);
      assertFuture.get();

      database.begin();
      ODocument externalDocTwo = new ODocument("NestedTxRollbackOne");
      externalDocTwo.field("v", "val2");
      externalDocTwo.save();

      assertFuture = executorService.submit(assertEmptyRecord);
      assertFuture.get();

      brokenDocOne.setDirty();
      brokenDocOne.save();

      database.commit();

      assertFuture = executorService.submit(assertEmptyRecord);
      assertFuture.get();

      final ODocument externalDocThree = new ODocument("NestedTxRollbackOne");
      externalDocThree.field("v", "val3");
      externalDocThree.save();

      database.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
      database.rollback();
    }

    Assert.assertTrue(!database.getTransaction().isActive());
    Assert.assertEquals(database.countClass("NestedTxRollbackOne"), 1);
  }

  public void testNestedTxRollbackTwo() {
    final OSchema schema = database.getMetadata().getSchema();
    if (!schema.existsClass("NestedTxRollbackTwo")) schema.createClass("NestedTxRollbackTwo");

    database.begin();
    try {
      final ODocument externalDocOne = new ODocument("NestedTxRollbackTwo");
      externalDocOne.field("v", "val1");
      externalDocOne.save();

      database.begin();

      final ODocument externalDocTwo = new ODocument("NestedTxRollbackTwo");
      externalDocTwo.field("v", "val2");
      externalDocTwo.save();

      database.rollback();

      database.begin();

      final ODocument externalDocFour = new ODocument("NestedTxRollbackTwo");
      externalDocFour.field("v", "val4");
      externalDocFour.save();

      database.commit();

      final ODocument externalDocThree = new ODocument("NestedTxRollbackTwo");
      externalDocThree.field("v", "val3");
      externalDocThree.save();

      database.commit();
      Assert.fail();
    } catch (ORollbackException e) {
      database.rollback();
    }

    Assert.assertTrue(!database.getTransaction().isActive());
    Assert.assertEquals(database.countClass("NestedTxRollbackTwo"), 0);
  }
}
