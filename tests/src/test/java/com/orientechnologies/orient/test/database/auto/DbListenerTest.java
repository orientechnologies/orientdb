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
import org.testng.annotations.*;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

/**
 * Tests the right calls of all the db's listener API.
 * 
 * @author Sylvain Spinelli
 * 
 */
public class DbListenerTest extends DocumentDBBaseTest {

  protected int onAfterTxCommit    = 0;
  protected int onAfterTxRollback  = 0;
  protected int onBeforeTxBegin    = 0;
  protected int onBeforeTxCommit   = 0;
  protected int onBeforeTxRollback = 0;
  protected int onClose            = 0;
  protected int onCreate           = 0;
  protected int onDelete           = 0;
  protected int onOpen             = 0;
  protected int onCorruption       = 0;

  public class DbListener implements ODatabaseListener {
    @Override
    public void onAfterTxCommit(ODatabase iDatabase) {
      onAfterTxCommit++;
    }

    @Override
    public void onAfterTxRollback(ODatabase iDatabase) {
      onAfterTxRollback++;
    }

    @Override
    public void onBeforeTxBegin(ODatabase iDatabase) {
      onBeforeTxBegin++;
    }

    @Override
    public void onBeforeTxCommit(ODatabase iDatabase) {
      onBeforeTxCommit++;
    }

    @Override
    public void onBeforeTxRollback(ODatabase iDatabase) {
      onBeforeTxRollback++;
    }

    @Override
    public void onClose(ODatabase iDatabase) {
      onClose++;
    }

    @Override
    public void onCreate(ODatabase iDatabase) {
      onCreate++;
    }

    @Override
    public void onDelete(ODatabase iDatabase) {
      onDelete++;
    }

    @Override
    public void onOpen(ODatabase iDatabase) {
      onOpen++;
    }

    @Override
    public boolean onCorruptionRepairDatabase(ODatabase iDatabase, final String iReason, String iWhatWillbeFixed) {
      onCorruption++;
      return true;
    }
  }

  @Parameters(value = "url")
  public DbListenerTest(@Optional String url) {
    super(url);
  }

  @AfterClass
  @Override
  public void afterClass() throws Exception {
  }

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {
  }

  @AfterMethod
  @Override
  public void afterMethod() throws Exception {
  }

  @Test
  public void testEmbeddedDbListeners() throws IOException {
    if (database.getURL().startsWith("remote:"))
      return;

    if (database.exists())
      ODatabaseHelper.deleteDatabase(database, getStorageType());

    database.registerListener(new DbListener());

    ODatabaseHelper.createDatabase(database, url, getStorageType());

    Assert.assertEquals(onCreate, 1);

    database.close();
    Assert.assertEquals(onClose, 1);

    database.open("admin", "admin");
    Assert.assertEquals(onOpen, 1);

    database.begin(TXTYPE.OPTIMISTIC);
    Assert.assertEquals(onBeforeTxBegin, 1);

    database.newInstance().save();
    database.commit();
    Assert.assertEquals(onBeforeTxCommit, 1);
    Assert.assertEquals(onAfterTxCommit, 1);

    database.begin(TXTYPE.OPTIMISTIC);
    Assert.assertEquals(onBeforeTxBegin, 2);

    database.newInstance().save();
    database.rollback();
    Assert.assertEquals(onBeforeTxRollback, 1);
    Assert.assertEquals(onAfterTxRollback, 1);

    ODatabaseHelper.deleteDatabase(database, getStorageType());
    Assert.assertEquals(onClose, 2);
    Assert.assertEquals(onDelete, 1);

    ODatabaseHelper.createDatabase(database, url, getStorageType());
  }

  @Test
  public void testRemoteDbListeners() throws IOException {
    if (!database.getURL().startsWith("remote:"))
      return;

    database.close();

    database.registerListener(new DbListener());

    database.open("admin", "admin");
    Assert.assertEquals(onOpen, 1);

    database.begin(TXTYPE.OPTIMISTIC);
    Assert.assertEquals(onBeforeTxBegin, 1);

    database.newInstance().save();
    database.commit();
    Assert.assertEquals(onBeforeTxCommit, 1);
    Assert.assertEquals(onAfterTxCommit, 1);

    database.begin(TXTYPE.OPTIMISTIC);
    Assert.assertEquals(onBeforeTxBegin, 2);

    database.newInstance().save();
    database.rollback();
    Assert.assertEquals(onBeforeTxRollback, 1);
    Assert.assertEquals(onAfterTxRollback, 1);

    database.close();
    Assert.assertEquals(onClose, 1);
  }

  @Test
  public void testEmbeddedDbListenersTxRecords() throws IOException {
    if (database.getURL().startsWith("remote:"))
      return;

    if (database.exists())
      ODatabaseHelper.deleteDatabase(database, getStorageType());
    ODatabaseHelper.createDatabase(database, url, getStorageType());
    database.close();

    database.registerListener(new ODatabaseListener() {
      @Override
      public void onCreate(ODatabase iDatabase) {
      }

      @Override
      public void onDelete(ODatabase iDatabase) {
      }

      @Override
      public void onOpen(ODatabase iDatabase) {

      }

      @Override
      public void onBeforeTxBegin(ODatabase iDatabase) {

      }

      @Override
      public void onBeforeTxRollback(ODatabase iDatabase) {

      }

      @Override
      public void onAfterTxRollback(ODatabase iDatabase) {

      }

      @Override
      public void onBeforeTxCommit(ODatabase iDatabase) {
        OTransaction tx = ((ODatabaseDocumentTx) iDatabase).getTransaction();
        Iterable<? extends ORecordOperation> recs = tx.getCurrentRecordEntries();
        for (ORecordOperation op : recs) {
          ODocument doc = (ODocument) op.getRecord();
          for (String f : doc.getDirtyFields()) {
            final Object oldValue = doc.getOriginalValue(f);
            final Object newValue = doc.field(f);

            System.out.println("Old: " + oldValue + " -> " + newValue);
          }
        }
      }

      @Override
      public void onAfterTxCommit(ODatabase iDatabase) {

      }

      @Override
      public void onClose(ODatabase iDatabase) {

      }

      @Override
      public boolean onCorruptionRepairDatabase(ODatabase iDatabase, String iReason, String iWhatWillbeFixed) {
        return false;
      }
    });

    database.open("admin", "admin");

    database.begin(TXTYPE.OPTIMISTIC);
    ODocument rec = database.newInstance().field("name", "Jay").save();
    database.commit();

    database.begin(TXTYPE.OPTIMISTIC);
    rec.field("surname", "Miner").save();
    database.commit();

    ODatabaseHelper.deleteDatabase(database, getStorageType());
    ODatabaseHelper.createDatabase(database, url, getStorageType());
  }
}
