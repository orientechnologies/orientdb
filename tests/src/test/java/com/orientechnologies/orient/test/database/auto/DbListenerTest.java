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

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * Tests the right calls of all the db's listener API.
 *
 * @author Sylvain Spinelli
 */
public class DbListenerTest extends DocumentDBBaseTest {

  protected int onAfterTxCommit = 0;
  protected int onAfterTxRollback = 0;
  protected int onBeforeTxBegin = 0;
  protected int onBeforeTxCommit = 0;
  protected int onBeforeTxRollback = 0;
  protected int onClose = 0;
  protected int onCreate = 0;
  protected int onDelete = 0;
  protected int onOpen = 0;
  protected int onCorruption = 0;
  protected String command;
  protected Object commandResult;

  public class DocumentChangeListener {
    final Map<ODocument, List<String>> changes = new HashMap<ODocument, List<String>>();

    public DocumentChangeListener(OrientBaseGraph g) {
      this(g.getRawGraph());
    }

    public DocumentChangeListener(final ODatabaseDocument db) {
      db.registerHook(
          new ODocumentHookAbstract(db) {

            @Override
            public ORecordHook.DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
              return ORecordHook.DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
            }

            @Override
            public void onRecordAfterUpdate(ODocument iDocument) {
              List<String> changedFields = new ArrayList<String>();
              for (String f : iDocument.getDirtyFields()) {
                changedFields.add(f);

                final Object oldValue = iDocument.getOriginalValue(f);
                final Object newValue = iDocument.field(f);

                // System.out.println("Field " + f + " Old: " + oldValue + " -> " + newValue);
              }
              changes.put(iDocument, changedFields);
            }
          });
    }

    public Map<ODocument, List<String>> getChanges() {
      return changes;
    }
  }

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
    public void onBeforeCommand(OCommandRequestText iCommand, OCommandExecutor executor) {
      command = iCommand.getText();
    }

    @Override
    public void onAfterCommand(
        OCommandRequestText iCommand, OCommandExecutor executor, Object result) {
      commandResult = result;
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
    public boolean onCorruptionRepairDatabase(
        ODatabase iDatabase, final String iReason, String iWhatWillbeFixed) {
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
  public void afterClass() throws Exception {}

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {}

  @AfterMethod
  @Override
  public void afterMethod() throws Exception {}

  @Test
  public void testEmbeddedDbListeners() throws IOException {
    if (database.getURL().startsWith("remote:")) return;

    if (database.exists()) ODatabaseHelper.deleteDatabase(database, getStorageType());

    database.registerListener(new DbListener());
    final int baseOnClose = onClose;
    final int baseOnCreate = onCreate;
    final int baseOnDelete = onDelete;

    ODatabaseHelper.createDatabase(database, url, getStorageType());

    final int baseOnBeforeTxBegin = onBeforeTxBegin;
    final int baseOnBeforeTxCommit = onBeforeTxCommit;
    final int baseOnAfterTxCommit = onAfterTxCommit;

    Assert.assertEquals(onCreate, baseOnCreate + 1);

    database.open("admin", "admin");
    Assert.assertEquals(onOpen, 1);

    database.begin(TXTYPE.OPTIMISTIC);
    Assert.assertEquals(onBeforeTxBegin, baseOnBeforeTxBegin + 1);

    database
        .<ODocument>newInstance()
        .save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();
    Assert.assertEquals(onBeforeTxCommit, baseOnBeforeTxCommit + 1);
    Assert.assertEquals(onAfterTxCommit, baseOnAfterTxCommit + 1);

    database.begin(TXTYPE.OPTIMISTIC);
    Assert.assertEquals(onBeforeTxBegin, baseOnBeforeTxBegin + 2);

    database
        .<ODocument>newInstance()
        .save(database.getClusterNameById(database.getDefaultClusterId()));
    database.rollback();
    Assert.assertEquals(onBeforeTxRollback, 1);
    Assert.assertEquals(onAfterTxRollback, 1);

    ODatabaseHelper.deleteDatabase(database, getStorageType());
    Assert.assertEquals(onClose, baseOnClose + 1);
    Assert.assertEquals(onDelete, baseOnDelete + 1);

    ODatabaseHelper.createDatabase(database, url, getStorageType());
  }

  @Test
  public void testRemoteDbListeners() throws IOException {
    if (!database.getURL().startsWith("remote:")) return;

    if (database.exists()) ODatabaseHelper.deleteDatabase(database, getStorageType());
    ODatabaseHelper.createDatabase(database, url, getStorageType());

    database.registerListener(new DbListener());
    database.open("admin", "admin");
    Assert.assertEquals(onOpen, 1);

    database.begin(TXTYPE.OPTIMISTIC);
    Assert.assertEquals(onBeforeTxBegin, 1);

    database
        .<ODocument>newInstance()
        .save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();
    Assert.assertEquals(onBeforeTxCommit, 1);
    Assert.assertEquals(onAfterTxCommit, 1);

    database.begin(TXTYPE.OPTIMISTIC);
    Assert.assertEquals(onBeforeTxBegin, 2);

    database
        .<ODocument>newInstance()
        .save(database.getClusterNameById(database.getDefaultClusterId()));
    database.rollback();
    Assert.assertEquals(onBeforeTxRollback, 1);
    Assert.assertEquals(onAfterTxRollback, 1);

    database.close();
    Assert.assertEquals(onClose, 1);
  }

  @Test
  public void testEmbeddedDbListenersTxRecords() throws IOException {
    if (database.getURL().startsWith("remote:")) return;

    if (database.exists()) ODatabaseHelper.deleteDatabase(database, getStorageType());
    ODatabaseHelper.createDatabase(database, url, getStorageType());

    final AtomicInteger recordedChanges = new AtomicInteger();

    database.open("admin", "admin");

    database.begin(TXTYPE.OPTIMISTIC);
    ODocument rec =
        database
            .<ODocument>newInstance()
            .field("name", "Jay")
            .save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final DocumentChangeListener cl = new DocumentChangeListener(database);

    database.begin(TXTYPE.OPTIMISTIC);
    rec.field("surname", "Miner").save();
    database.commit();

    Assert.assertEquals(cl.getChanges().size(), 1);

    ODatabaseHelper.deleteDatabase(database, getStorageType());
    ODatabaseHelper.createDatabase(database, url, getStorageType());
  }

  @Test
  public void testEmbeddedDbListenersGraph() throws IOException {
    if (database.getURL().startsWith("remote:")) return;

    if (database.exists()) ODatabaseHelper.deleteDatabase(database, getStorageType());
    ODatabaseHelper.createDatabase(database, url, getStorageType());

    database.open("admin", "admin");
    OrientGraph g = new OrientGraph(database);
    OrientVertex v = g.addVertex(null);
    v.setProperty("name", "Jay");
    g.commit();

    final DocumentChangeListener cl = new DocumentChangeListener(g);

    v.setProperty("surname", "Miner");
    g.shutdown();

    Assert.assertEquals(cl.getChanges().size(), 1);

    ODatabaseHelper.deleteDatabase(database, getStorageType());
    ODatabaseHelper.createDatabase(database, url, getStorageType());
  }

  @Test
  public void testEmbeddedDbListenersCommands() throws IOException {

    if (database.getURL().startsWith("remote:")) return;

    if (database.exists()) ODatabaseHelper.deleteDatabase(database, getStorageType());
    ODatabaseHelper.createDatabase(database, url, getStorageType());

    final AtomicInteger recordedChanges = new AtomicInteger();

    database.open("admin", "admin");

    database.registerListener(new DbListener());

    String iText = "select from OUser";
    Object execute = database.command(new OSQLSynchQuery<Object>(iText)).execute();

    Assert.assertEquals(execute, commandResult);
    Assert.assertEquals(iText, command);
    ODatabaseHelper.deleteDatabase(database, getStorageType());
    ODatabaseHelper.createDatabase(database, url, getStorageType());
  }
}
