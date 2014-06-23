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

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.client.remote.OStorageRemoteThread;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.ORemoteServerEventListener;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * Tests the right calls of all the db's listener API.
 * 
 * @author Sylvain Spinelli
 * 
 */
public class DbListenerTest {
  protected ODatabaseDocumentTx database;

  protected String              dbUrl;

  protected int                 onAfterTxCommit              = 0;
  protected int                 onAfterTxRollback            = 0;
  protected int                 onBeforeTxBegin              = 0;
  protected int                 onBeforeTxCommit             = 0;
  protected int                 onBeforeTxRollback           = 0;
  protected int                 onClose                      = 0;
  protected int                 onCreate                     = 0;
  protected int                 onDelete                     = 0;
  protected int                 onOpen                       = 0;
  protected int                 onCorruption                 = 0;

  protected int                 onRecordPulled               = 0;
  protected int                 onClusterConfigurationChange = 0;
  protected int                 onAvailableDatabaseChange    = 0;

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
  public DbListenerTest(String iURL) {
    dbUrl = iURL;
    database = new ODatabaseDocumentTx(iURL);
  }

  @Test
  public void testEmbeddedDbListeners() throws IOException {
    if (database.getURL().startsWith("remote:"))
      return;
    if (database.exists())
      ODatabaseHelper.deleteDatabase(database, "plocal");

    database.registerListener(new DbListener());

    ODatabaseHelper.createDatabase(database, dbUrl, "plocal");

    Assert.assertEquals(onCreate, 1);

    database.close();
    Assert.assertEquals(onClose, 1);

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

    ODatabaseHelper.deleteDatabase(database, "plocal");
    Assert.assertEquals(onClose, 2);
    Assert.assertEquals(onDelete, 1);
  }

  @Test
  public void testRemoteDbListeners() throws IOException {
    if (!database.getURL().startsWith("remote:"))
      return;

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
}
