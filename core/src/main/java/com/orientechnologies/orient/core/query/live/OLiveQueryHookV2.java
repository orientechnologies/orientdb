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
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OLiveQueryHookV2 {

  public static class OLiveQueryOp {
    public    OResult   before;
    public    OResult   after;
    public    byte      type;
    protected ODocument originalDoc;

    OLiveQueryOp(ODocument originalDoc, OResult before, OResult after, byte type) {
      this.originalDoc = originalDoc;
      this.type = type;
      this.before = before;
      this.after = after;
    }
  }

  public static class OLiveQueryOps implements OCloseable {

    protected Map<ODatabaseDocument, List<OLiveQueryOp>> pendingOps  = new ConcurrentHashMap<ODatabaseDocument, List<OLiveQueryOp>>();
    private   OLiveQueryQueueThreadV2                    queueThread = new OLiveQueryQueueThreadV2();
    private   Object                                     threadLock  = new Object();

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

    public OLiveQueryQueueThreadV2 getQueueThread() {
      return queueThread;
    }
  }

  public static OLiveQueryOps getOpsReference(ODatabaseInternal db) {
    return db.getSharedContext().getLiveQueryOpsV2();
  }

  public static Integer subscribe(Integer token, OLiveQueryListenerV2 iListener, ODatabaseInternal db) {
    if (Boolean.FALSE.equals(db.getConfiguration().getValue(OGlobalConfiguration.QUERY_LIVE_SUPPORT))) {
      OLogManager.instance().warn(db,
          "Live query support is disabled impossible to subscribe a listener, set '%s' to true for enable the live query support",
          OGlobalConfiguration.QUERY_LIVE_SUPPORT.getKey());
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
    if (Boolean.FALSE.equals(db.getConfiguration().getValue(OGlobalConfiguration.QUERY_LIVE_SUPPORT))) {
      OLogManager.instance().warn(db,
          "Live query support is disabled impossible to unsubscribe a listener, set '%s' to true for enable the live query support",
          OGlobalConfiguration.QUERY_LIVE_SUPPORT.getKey());
      return;
    }
    try {
      OLiveQueryOps ops = getOpsReference(db);
      synchronized (ops.threadLock) {
        ops.queueThread.unsubscribe(id);
      }
    } catch (Exception e) {
      OLogManager.instance().warn(OLiveQueryHookV2.class, "Error on unsubscribing client", e);
    }
  }

  public static void notifyForTxChanges(ODatabaseDocument database) {
    if (Boolean.FALSE.equals(database.getConfiguration().getValue(OGlobalConfiguration.QUERY_LIVE_SUPPORT)))
      return;
    OLiveQueryOps ops = getOpsReference((ODatabaseInternal) database);
    List<OLiveQueryOp> list;
    synchronized (ops.pendingOps) {
      list = ops.pendingOps.remove(database);
    }
    // TODO sync
    if (list != null) {
      for (OLiveQueryOp item : list) {
        item.originalDoc = item.originalDoc.copy();
        ops.queueThread.enqueue(item);
      }
    }
  }

  public static void removePendingDatabaseOps(ODatabaseDocument database) {
    if (Boolean.FALSE.equals(database.getConfiguration().getValue(OGlobalConfiguration.QUERY_LIVE_SUPPORT)))
      return;
    OLiveQueryOps ops = getOpsReference((ODatabaseInternal) database);
    synchronized (ops.pendingOps) {
      ops.pendingOps.remove(database);
    }
  }

  public static void addOp(ODocument iDocument, byte iType, ODatabaseDocument database) {
    if (Boolean.FALSE.equals(database.getConfiguration().getValue(OGlobalConfiguration.QUERY_LIVE_SUPPORT)))
      return;
    ODatabaseDocument db = database;
    OLiveQueryOps ops = getOpsReference((ODatabaseInternal) db);
    if (!ops.queueThread.hasListeners())
      return;

    OResult before = iType == ORecordOperation.CREATED ? null : calculateBefore(iDocument);
    OResult after = iType == ORecordOperation.DELETED ? null : calculateAfter(iDocument);

    OLiveQueryOp result = new OLiveQueryOp(iDocument, before, after, iType);
    synchronized (ops.pendingOps) {
      List<OLiveQueryOp> list = ops.pendingOps.get(db);
      if (list == null) {
        list = new ArrayList<>();
        ops.pendingOps.put(db, list);
      }
      if (result.type == ORecordOperation.UPDATED) {
        OLiveQueryOp prev = prevousUpdate(list, result.originalDoc);
        if (prev == null) {
          list.add(result);
        } else {
          prev.after = result.after;
        }
      } else {
        list.add(result);
      }
    }
  }

  private static OLiveQueryOp prevousUpdate(List<OLiveQueryOp> list, ODocument doc) {
    for (OLiveQueryOp oLiveQueryOp : list) {
      if (oLiveQueryOp.originalDoc == doc) {
        return oLiveQueryOp;
      }
    }
    return null;
  }

  private static OResultInternal calculateBefore(ODocument iDocument) {
    OResultInternal result = new OResultInternal();
    for (String prop : iDocument.getPropertyNames()) {
      result.setProperty(prop, iDocument.getProperty(prop));
    }
    result.setProperty("@rid", iDocument.getIdentity());
    result.setProperty("@class", iDocument.getClassName());
    result.setProperty("@version", iDocument.getVersion());
    for (String prop : iDocument.getDirtyFields()) {
      result.setProperty(prop, iDocument.getOriginalValue(prop));
    }
    return result;
  }

  private static OResultInternal calculateAfter(ODocument iDocument) {
    OResultInternal result = new OResultInternal();
    for (String prop : iDocument.getPropertyNames()) {
      result.setProperty(prop, iDocument.getProperty(prop));
    }
    result.setProperty("@rid", iDocument.getIdentity());
    result.setProperty("@class", iDocument.getClassName());
    result.setProperty("@version", iDocument.getVersion() + 1);
    return result;
  }
}
