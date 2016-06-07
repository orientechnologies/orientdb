/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.query.live;

import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by luigidellaquila on 16/03/15.
 */
public class OLiveQueryHook extends ODocumentHookAbstract implements ODatabaseListener {

  static class OLiveQueryOps implements OCloseable {

    protected Map<ODatabaseDocument, List<ORecordOperation>> pendingOps  = new ConcurrentHashMap<ODatabaseDocument, List<ORecordOperation>>();
    private OLiveQueryQueueThread                            queueThread = new OLiveQueryQueueThread();
    private Object                                           threadLock  = new Object();

    @Override
    public void close() {
      queueThread.stopExecution();
      try {
        queueThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      pendingOps.clear();
    }
  }

  public OLiveQueryHook(ODatabaseDocumentInternal db) {
    super(db);
    getOpsReference(db);
    db.registerListener(this);
  }

  private static OLiveQueryOps getOpsReference(ODatabaseInternal db) {
    return (OLiveQueryOps) db.getStorage().getResource("LiveQueryOps", new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return new OLiveQueryOps();
      }
    });
  }

  public static Integer subscribe(Integer token, OLiveQueryListener iListener, ODatabaseInternal db) {
    if(Boolean.FALSE.equals(db.getConfiguration().getValue(OGlobalConfiguration.QUERY_LIVE_SUPPORT))) {
      OLogManager.instance().warn(db,"Live query support is disabled impossible to subscribe a listener, set '%s' to true for enable the live query support", OGlobalConfiguration.QUERY_LIVE_SUPPORT.getKey());
      return -1;
    }
    OLiveQueryOps ops = getOpsReference(db);
    synchronized (ops.threadLock) {
      if (!ops.queueThread.isAlive()) {
        ops.queueThread = ops.queueThread.clone();
        ops.queueThread.start();
      }
    }

    return ops.queueThread.subscribe(token, iListener);
  }

  public static void unsubscribe(Integer id, ODatabaseInternal db) {
    if(Boolean.FALSE.equals(db.getConfiguration().getValue(OGlobalConfiguration.QUERY_LIVE_SUPPORT))) {
      OLogManager.instance().warn(db, "Live query support is disabled impossible to unsubscribe a listener, set '%s' to true for enable the live query support", OGlobalConfiguration.QUERY_LIVE_SUPPORT.getKey());
      return;
    }
    try {
      OLiveQueryOps ops = getOpsReference(db);
      synchronized (ops.threadLock) {
        ops.queueThread.unsubscribe(id);
      }
    } catch (Exception e) {
      OLogManager.instance().warn(OLiveQueryHook.class, "Error on unsubscribing client");
    }
  }

  @Override
  public void onCreate(ODatabase iDatabase) {

  }

  @Override
  public void onDelete(ODatabase iDatabase) {
    if(Boolean.FALSE.equals(database.getConfiguration().getValue(OGlobalConfiguration.QUERY_LIVE_SUPPORT)))
      return ;
    OLiveQueryOps ops = getOpsReference((ODatabaseInternal) iDatabase);
    synchronized (ops.pendingOps) {
      ops.pendingOps.remove(iDatabase);
    }
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
    if(Boolean.FALSE.equals(database.getConfiguration().getValue(OGlobalConfiguration.QUERY_LIVE_SUPPORT)))
      return ;
    OLiveQueryOps ops = getOpsReference((ODatabaseInternal) iDatabase);
    synchronized (ops.pendingOps) {
      ops.pendingOps.remove(iDatabase);
    }
  }

  @Override
  public void onBeforeTxCommit(ODatabase iDatabase) {

  }

  @Override
  public void onAfterTxCommit(ODatabase iDatabase) {
    if(Boolean.FALSE.equals(database.getConfiguration().getValue(OGlobalConfiguration.QUERY_LIVE_SUPPORT)))
      return ;
    OLiveQueryOps ops = getOpsReference((ODatabaseInternal) iDatabase);
    List<ORecordOperation> list;
    synchronized (ops.pendingOps) {
      list = ops.pendingOps.remove(iDatabase);
    }
    // TODO sync
    if (list != null) {
      for (ORecordOperation item : list) {
        item.setRecord(item.getRecord().copy());
        ops.queueThread.enqueue(item);
      }
    }
  }

  @Override
  public void onClose(ODatabase iDatabase) {
    if(Boolean.FALSE.equals(database.getConfiguration().getValue(OGlobalConfiguration.QUERY_LIVE_SUPPORT)))
      return ;
    OLiveQueryOps ops = getOpsReference((ODatabaseInternal) iDatabase);
    synchronized (ops.pendingOps) {
      ops.pendingOps.remove(iDatabase);
    }
  }

  @Override
  public void onBeforeCommand(OCommandRequestText iCommand, OCommandExecutor executor) {

  }

  @Override
  public void onAfterCommand(OCommandRequestText iCommand, OCommandExecutor executor, Object result) {

  }

  @Override
  public void onRecordAfterCreate(ODocument iDocument) {
    addOp(iDocument, ORecordOperation.CREATED);
  }

  @Override
  public void onRecordAfterUpdate(ODocument iDocument) {
    addOp(iDocument, ORecordOperation.UPDATED);
  }

  @Override
  public RESULT onRecordBeforeDelete(ODocument iDocument) {
    addOp(iDocument, ORecordOperation.DELETED);
    return RESULT.RECORD_NOT_CHANGED;
  }

  protected void addOp(ODocument iDocument, byte iType) {
    if(Boolean.FALSE.equals(database.getConfiguration().getValue(OGlobalConfiguration.QUERY_LIVE_SUPPORT)))
      return ;
    ODatabaseDocument db = database;
    OLiveQueryOps ops = getOpsReference((ODatabaseInternal) db);
    if(!ops.queueThread.hasListeners())
      return;
    if (db.getTransaction() == null || !db.getTransaction().isActive()) {

      // TODO synchronize
      ORecordOperation op = new ORecordOperation(iDocument.copy(), iType);
      ops.queueThread.enqueue(op);
      return;
    }
    ORecordOperation result = new ORecordOperation(iDocument, iType);
    synchronized (ops.pendingOps) {
      List<ORecordOperation> list = ops.pendingOps.get(db);
      if (list == null) {
        list = new ArrayList<ORecordOperation>();
        ops.pendingOps.put(db, list);
      }
      list.add(result);
    }
  }

  @Override
  public boolean onCorruptionRepairDatabase(ODatabase iDatabase, String iReason, String iWhatWillbeFixed) {
    return false;
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.BOTH;
  }
}
