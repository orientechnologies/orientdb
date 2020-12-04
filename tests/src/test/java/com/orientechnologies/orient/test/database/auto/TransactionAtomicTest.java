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

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "dictionary")
public class TransactionAtomicTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public TransactionAtomicTest(@Optional String url) {
    super(url);
  }

  @Test
  public void testTransactionAtomic() throws IOException {
    ODatabaseDocumentTx db1 = new ODatabaseDocumentTx(url);
    db1.open("admin", "admin");

    ODatabaseDocumentTx db2 = new ODatabaseDocumentTx(url);
    db2.open("admin", "admin");

    ODocument record1 = new ODocument();
    record1
        .field("value", "This is the first version")
        .save(db2.getClusterNameById(db2.getDefaultClusterId()));

    // RE-READ THE RECORD
    record1.reload();

    db2.activateOnCurrentThread();
    ODocument record2 = db2.load(record1.getIdentity());

    record2.field("value", "This is the second version").save();
    record2.field("value", "This is the third version").save();

    db1.activateOnCurrentThread();
    record1.reload(null, true);

    Assert.assertEquals(record1.field("value"), "This is the third version");

    db1.close();

    db2.activateOnCurrentThread();
    db2.close();

    database.activateOnCurrentThread();
  }

  @Test
  public void testMVCC() throws IOException {

    ODocument doc = new ODocument("Account");
    doc.field("version", 0);
    doc.save();

    doc.setDirty();
    doc.field("testmvcc", true);
    ORecordInternal.setVersion(doc, doc.getVersion() + 1);
    try {
      doc.save();
      Assert.assertTrue(false);
    } catch (OConcurrentModificationException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testTransactionPreListenerRollback() throws IOException {
    ODocument record1 = new ODocument();
    record1
        .field("value", "This is the first version")
        .save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODatabaseListener listener =
        new ODatabaseListener() {

          @Override
          public void onAfterTxCommit(ODatabase iDatabase) {}

          @Override
          public void onAfterTxRollback(ODatabase iDatabase) {}

          @Override
          public void onBeforeTxBegin(ODatabase iDatabase) {}

          @Override
          public void onBeforeTxCommit(ODatabase iDatabase) {
            throw new RuntimeException("Rollback test");
          }

          @Override
          public void onBeforeTxRollback(ODatabase iDatabase) {}

          @Override
          public void onClose(ODatabase iDatabase) {}

          @Override
          public void onBeforeCommand(OCommandRequestText iCommand, OCommandExecutor executor) {}

          @Override
          public void onAfterCommand(
              OCommandRequestText iCommand, OCommandExecutor executor, Object result) {}

          @Override
          public void onCreate(ODatabase iDatabase) {}

          @Override
          public void onDelete(ODatabase iDatabase) {}

          @Override
          public void onOpen(ODatabase iDatabase) {}

          @Override
          public boolean onCorruptionRepairDatabase(
              ODatabase iDatabase, final String iReason, String iWhatWillbeFixed) {
            return true;
          }
        };

    database.registerListener(listener);
    database.begin();

    try {
      database.commit();
      Assert.assertTrue(false);
    } catch (OTransactionException e) {
      Assert.assertTrue(true);
    } finally {
      database.unregisterListener(listener);
    }
  }

  @Test
  public void testTransactionWithDuplicateUniqueIndexValues() {
    OClass fruitClass = database.getMetadata().getSchema().getClass("Fruit");

    if (fruitClass == null) {
      fruitClass = database.getMetadata().getSchema().createClass("Fruit");

      fruitClass.createProperty("name", OType.STRING);
      fruitClass.createProperty("color", OType.STRING);

      database
          .getMetadata()
          .getSchema()
          .getClass("Fruit")
          .getProperty("color")
          .createIndex(OClass.INDEX_TYPE.UNIQUE);
    }

    Assert.assertEquals(database.countClusterElements("Fruit"), 0);

    try {
      database.begin();

      ODocument apple = new ODocument("Fruit").field("name", "Apple").field("color", "Red");
      ODocument orange = new ODocument("Fruit").field("name", "Orange").field("color", "Orange");
      ODocument banana = new ODocument("Fruit").field("name", "Banana").field("color", "Yellow");
      ODocument kumquat = new ODocument("Fruit").field("name", "Kumquat").field("color", "Orange");

      apple.save();
      orange.save();
      banana.save();
      kumquat.save();

      database.commit();

      Assert.assertEquals(apple.getIdentity().getClusterId(), fruitClass.getDefaultClusterId());
      Assert.assertEquals(orange.getIdentity().getClusterId(), fruitClass.getDefaultClusterId());
      Assert.assertEquals(banana.getIdentity().getClusterId(), fruitClass.getDefaultClusterId());
      Assert.assertEquals(kumquat.getIdentity().getClusterId(), fruitClass.getDefaultClusterId());

      Assert.assertTrue(false);

    } catch (ORecordDuplicatedException e) {
      Assert.assertTrue(true);
      database.rollback();
    }

    Assert.assertEquals(database.countClusterElements("Fruit"), 0);
  }

  @Test
  public void testTransactionalSQL() {
    long prev = database.countClass("Account");

    database
        .command(new OCommandSQL("transactional insert into Account set name = 'txTest1'"))
        .execute();

    Assert.assertEquals(database.countClass("Account"), prev + 1);
  }

  @Test
  public void testTransactionalSQLJoinTx() {
    long prev = database.countClass("Account");

    database.begin();

    database
        .command(new OCommandSQL("transactional insert into Account set name = 'txTest2'"))
        .execute();

    Assert.assertTrue(database.getTransaction().isActive());

    if (!url.startsWith("remote")) Assert.assertEquals(database.countClass("Account"), prev + 1);

    database.commit();

    Assert.assertFalse(database.getTransaction().isActive());
    Assert.assertEquals(database.countClass("Account"), prev + 1);
  }
}
