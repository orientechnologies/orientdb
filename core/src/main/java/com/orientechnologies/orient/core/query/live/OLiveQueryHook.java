/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.query.live;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.QUERY_LIVE_SUPPORT;

import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Created by luigidellaquila on 16/03/15. */
public class OLiveQueryHook {

  public static class OLiveQueryOps implements OCloseable {

    protected Map<ODatabaseDocument, List<ORecordOperation>> pendingOps =
        new ConcurrentHashMap<ODatabaseDocument, List<ORecordOperation>>();
    private OLiveQueryQueueThread queueThread = new OLiveQueryQueueThread();
    private Object threadLock = new Object();

    @Override
    public void close() {
      queueThread.stopExecution();
      try {
        queueThread.join();
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
      pendingOps.clear();
    }

    public OLiveQueryQueueThread getQueueThread() {
      return queueThread;
    }
  }

  public static OLiveQueryOps getOpsReference(ODatabaseInternal db) {
    return db.getSharedContext().getLiveQueryOps();
  }

  public static Integer subscribe(
      Integer token, OLiveQueryListener iListener, ODatabaseInternal db) {
    if (Boolean.FALSE.equals(db.getConfiguration().getValue(QUERY_LIVE_SUPPORT))) {
      OLogManager.instance()
          .warn(
              db,
              "Live query support is disabled impossible to subscribe a listener, set '%s' to true for enable the live query support",
              QUERY_LIVE_SUPPORT.getKey());
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
    if (Boolean.FALSE.equals(db.getConfiguration().getValue(QUERY_LIVE_SUPPORT))) {
      OLogManager.instance()
          .warn(
              db,
              "Live query support is disabled impossible to unsubscribe a listener, set '%s' to true for enable the live query support",
              QUERY_LIVE_SUPPORT.getKey());
      return;
    }
    try {
      OLiveQueryOps ops = getOpsReference(db);
      synchronized (ops.threadLock) {
        ops.queueThread.unsubscribe(id);
      }
    } catch (Exception e) {
      OLogManager.instance().warn(OLiveQueryHook.class, "Error on unsubscribing client", e);
    }
  }

  public static void notifyForTxChanges(ODatabase iDatabase) {

    OLiveQueryOps ops = getOpsReference((ODatabaseInternal) iDatabase);
    if (ops.pendingOps.isEmpty()) {
      return;
    }
    if (Boolean.FALSE.equals(iDatabase.getConfiguration().getValue(QUERY_LIVE_SUPPORT))) return;

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

  public static void removePendingDatabaseOps(ODatabase iDatabase) {
    try {
      if (iDatabase.isClosed()
          || Boolean.FALSE.equals(iDatabase.getConfiguration().getValue(QUERY_LIVE_SUPPORT)))
        return;
      OLiveQueryOps ops = getOpsReference((ODatabaseInternal) iDatabase);
      synchronized (ops.pendingOps) {
        ops.pendingOps.remove(iDatabase);
      }
    } catch (ODatabaseException ex) {
      // This catch and log the exception because in some case is suppressing the real exception
      OLogManager.instance().error(iDatabase, "Error cleaning the live query resources", ex);
    }
  }

  public static void addOp(ODocument iDocument, byte iType, ODatabaseDocument database) {
    ODatabaseDocument db = database;
    OLiveQueryOps ops = getOpsReference((ODatabaseInternal) db);
    if (!ops.queueThread.hasListeners()) return;
    if (Boolean.FALSE.equals(database.getConfiguration().getValue(QUERY_LIVE_SUPPORT))) return;
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
}
