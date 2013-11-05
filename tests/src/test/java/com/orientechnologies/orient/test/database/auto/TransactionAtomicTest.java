/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseFlat;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;

@Test(groups = "dictionary")
public class TransactionAtomicTest {
  private String url;

  @Parameters(value = "url")
  public TransactionAtomicTest(String iURL) {
    url = iURL;
  }

  @Test
  public void testTransactionAtomic() throws IOException {
    ODatabaseFlat db1 = new ODatabaseFlat(url);
    db1.open("admin", "admin");

    ODatabaseFlat db2 = new ODatabaseFlat(url);
    db2.open("admin", "admin");

    ORecordFlat record1 = new ORecordFlat(db1);
    record1.value("This is the first version").save();

    // RE-READ THE RECORD
    record1.reload();
    ORecordFlat record2 = db2.load(record1.getIdentity());

    record2.value("This is the second version").save();
    record2.value("This is the third version").save();

    record1.reload(null, true);

    Assert.assertEquals(record1.value(), "This is the third version");

    db1.close();
    db2.close();
  }

  @Test
  public void testMVCC() throws IOException {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    db.open("admin", "admin");

    ODocument doc = new ODocument("Account");
    doc.field("version", 0);
    doc.save();

    doc.setDirty();
    doc.field("testmvcc", true);
    doc.getRecordVersion().increment();
    try {
      doc.save();
      Assert.assertTrue(false);
    } catch (OConcurrentModificationException e) {
      Assert.assertTrue(true);
    }

    db.close();
  }

  @Test(expectedExceptions = OTransactionException.class)
  public void testTransactionPreListenerRollback() throws IOException {
    ODatabaseFlat db = new ODatabaseFlat(url);
    db.open("admin", "admin");

    ORecordFlat record1 = new ORecordFlat(db);
    record1.value("This is the first version").save();

    db.registerListener(new ODatabaseListener() {

      public void onAfterTxCommit(ODatabase iDatabase) {
      }

      public void onAfterTxRollback(ODatabase iDatabase) {
      }

      public void onBeforeTxBegin(ODatabase iDatabase) {
      }

      public void onBeforeTxCommit(ODatabase iDatabase) {
        throw new RuntimeException("Rollback test");
      }

      public void onBeforeTxRollback(ODatabase iDatabase) {
      }

      public void onClose(ODatabase iDatabase) {
      }

      public void onCreate(ODatabase iDatabase) {
      }

      public void onDelete(ODatabase iDatabase) {
      }

      public void onOpen(ODatabase iDatabase) {
      }

      public boolean onCorruptionRepairDatabase(ODatabase iDatabase, final String iReason, String iWhatWillbeFixed) {
        return true;
      }
    });

    db.commit();

    db.close();
  }

  @Test
  public void testTransactionWithDuplicateUniqueIndexValues() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    db.open("admin", "admin");

    OClass fruitClass = db.getMetadata().getSchema().getClass("Fruit");

    if (fruitClass == null) {
      fruitClass = db.getMetadata().getSchema().createClass("Fruit");

      fruitClass.createProperty("name", OType.STRING);
      fruitClass.createProperty("color", OType.STRING);

      db.getMetadata().getSchema().getClass("Fruit").getProperty("color").createIndex(OClass.INDEX_TYPE.UNIQUE);
    }

    Assert.assertEquals(db.countClusterElements("Fruit"), 0);

    try {
      db.begin();

      ODocument apple = new ODocument("Fruit").field("name", "Apple").field("color", "Red");
      ODocument orange = new ODocument("Fruit").field("name", "Orange").field("color", "Orange");
      ODocument banana = new ODocument("Fruit").field("name", "Banana").field("color", "Yellow");
      ODocument kumquat = new ODocument("Fruit").field("name", "Kumquat").field("color", "Orange");

      apple.save();
      Assert.assertEquals(apple.getIdentity().getClusterId(), fruitClass.getDefaultClusterId());

      orange.save();
      Assert.assertEquals(orange.getIdentity().getClusterId(), fruitClass.getDefaultClusterId());

      banana.save();
      Assert.assertEquals(banana.getIdentity().getClusterId(), fruitClass.getDefaultClusterId());

      kumquat.save();
      Assert.assertEquals(kumquat.getIdentity().getClusterId(), fruitClass.getDefaultClusterId());

      db.commit();
      Assert.assertTrue(false);

    } catch (ORecordDuplicatedException e) {
      Assert.assertTrue(true);
      db.rollback();

    }

    Assert.assertEquals(db.countClusterElements("Fruit"), 0);

    db.close();
  }

  @Test
  public void testTransactionalSQL() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    db.open("admin", "admin");

    long prev = db.countClusterElements("Account");

    db.command(new OCommandSQL("transactional insert into Account set name = 'txTest1'")).execute();

    Assert.assertEquals(db.countClusterElements("Account"), prev + 1);
    db.close();
  }

  @Test
  public void testTransactionalSQLJoinTx() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    db.open("admin", "admin");

    long prev = db.countClusterElements("Account");

    db.begin();

    db.command(new OCommandSQL("transactional insert into Account set name = 'txTest2'")).execute();

    Assert.assertTrue(db.getTransaction().isActive());

    if (!url.startsWith("remote"))
      Assert.assertEquals(db.countClusterElements("Account"), prev);

    db.commit();

    Assert.assertFalse(db.getTransaction().isActive());
    Assert.assertEquals(db.countClusterElements("Account"), prev + 1);

    db.close();
  }
}
