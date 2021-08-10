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
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.executor.LiveQueryListenerImpl;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.parser.OProjection;
import com.orientechnologies.orient.core.sql.parser.OProjectionItem;
import com.orientechnologies.orient.core.sql.parser.OSelectStatement;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public class OLiveQueryHookV2 {

  public static class OLiveQueryOp {
    public OResult before;
    public OResult after;
    public byte type;
    protected ODocument originalDoc;

    OLiveQueryOp(ODocument originalDoc, OResult before, OResult after, byte type) {
      this.originalDoc = originalDoc;
      this.type = type;
      this.before = before;
      this.after = after;
    }
  }

  public static class OLiveQueryOps implements OCloseable {

    protected Map<ODatabaseDocument, List<OLiveQueryOp>> pendingOps =
        new ConcurrentHashMap<ODatabaseDocument, List<OLiveQueryOp>>();
    private OLiveQueryQueueThreadV2 queueThread = new OLiveQueryQueueThreadV2(this);
    private Object threadLock = new Object();

    private BlockingQueue<OLiveQueryOp> queue = new LinkedBlockingQueue<OLiveQueryOp>();
    private ConcurrentMap<Integer, OLiveQueryListenerV2> subscribers =
        new ConcurrentHashMap<Integer, OLiveQueryListenerV2>();

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

    public Map<Integer, OLiveQueryListenerV2> getSubscribers() {
      return subscribers;
    }

    public BlockingQueue<OLiveQueryOp> getQueue() {
      return queue;
    }

    public void enqueue(OLiveQueryHookV2.OLiveQueryOp item) {
      queue.offer(item);
    }

    public Integer subscribe(Integer id, OLiveQueryListenerV2 iListener) {
      subscribers.put(id, iListener);
      return id;
    }

    public void unsubscribe(Integer id) {
      OLiveQueryListenerV2 res = subscribers.remove(id);
      if (res != null) {
        res.onLiveResultEnd();
      }
    }

    public boolean hasListeners() {
      return !subscribers.isEmpty();
    }
  }

  public static OLiveQueryOps getOpsReference(ODatabaseInternal db) {
    return db.getSharedContext().getLiveQueryOpsV2();
  }

  public static Integer subscribe(
      Integer token, OLiveQueryListenerV2 iListener, ODatabaseInternal db) {
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

    return ops.subscribe(token, iListener);
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
        ops.unsubscribe(id);
      }
    } catch (Exception e) {
      OLogManager.instance().warn(OLiveQueryHookV2.class, "Error on unsubscribing client", e);
    }
  }

  public static void notifyForTxChanges(ODatabaseDocument database) {
    OLiveQueryOps ops = getOpsReference((ODatabaseInternal) database);
    if (ops.pendingOps.isEmpty()) {
      return;
    }
    List<OLiveQueryOp> list;
    if (Boolean.FALSE.equals(database.getConfiguration().getValue(QUERY_LIVE_SUPPORT))) return;
    synchronized (ops.pendingOps) {
      list = ops.pendingOps.remove(database);
    }
    // TODO sync
    if (list != null) {
      for (OLiveQueryOp item : list) {
        item.originalDoc = item.originalDoc.copy();
        ops.enqueue(item);
      }
    }
  }

  public static void removePendingDatabaseOps(ODatabaseDocument database) {
    try {
      if (database.isClosed()
          || Boolean.FALSE.equals(database.getConfiguration().getValue(QUERY_LIVE_SUPPORT))) return;
      OLiveQueryOps ops = getOpsReference((ODatabaseInternal) database);
      synchronized (ops.pendingOps) {
        ops.pendingOps.remove(database);
      }
    } catch (ODatabaseException ex) {
      // This catch and log the exception because in some case is suppressing the real exception
      OLogManager.instance().error(database, "Error cleaning the live query resources", ex);
    }
  }

  public static void addOp(ODocument iDocument, byte iType, ODatabaseDocument database) {
    ODatabaseDocument db = database;
    OLiveQueryOps ops = getOpsReference((ODatabaseInternal) db);
    if (!ops.hasListeners()) return;
    if (Boolean.FALSE.equals(database.getConfiguration().getValue(QUERY_LIVE_SUPPORT))) return;

    Set<String> projectionsToLoad = calculateProjections(ops);

    OResult before =
        iType == ORecordOperation.CREATED ? null : calculateBefore(iDocument, projectionsToLoad);
    OResult after =
        iType == ORecordOperation.DELETED ? null : calculateAfter(iDocument, projectionsToLoad);

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

  /**
   * get all the projections that are needed by the live queries. Null means all
   *
   * @param ops
   * @return
   */
  private static Set<String> calculateProjections(OLiveQueryOps ops) {
    Set<String> result = new HashSet<>();
    if (ops == null || ops.subscribers == null) {
      return null;
    }
    for (OLiveQueryListenerV2 listener : ops.subscribers.values()) {
      if (listener instanceof LiveQueryListenerImpl) {
        OSelectStatement query = ((LiveQueryListenerImpl) listener).getStatement();
        OProjection proj = query.getProjection();
        if (proj == null || proj.getItems() == null || proj.getItems().isEmpty()) {
          return null;
        }
        for (OProjectionItem item : proj.getItems()) {
          if (!item.getExpression().isBaseIdentifier()) {
            return null;
          }
          result.add(item.getExpression().getDefaultAlias().getStringValue());
        }
      }
    }
    return result;
  }

  private static OLiveQueryOp prevousUpdate(List<OLiveQueryOp> list, ODocument doc) {
    for (OLiveQueryOp oLiveQueryOp : list) {
      if (oLiveQueryOp.originalDoc == doc) {
        return oLiveQueryOp;
      }
    }
    return null;
  }

  public static OResultInternal calculateBefore(
      ODocument iDocument, Set<String> projectionsToLoad) {
    OResultInternal result = new OResultInternal();
    for (String prop : iDocument.getPropertyNames()) {
      if (projectionsToLoad == null || projectionsToLoad.contains(prop)) {
        result.setProperty(prop, unboxRidbags(iDocument.getProperty(prop)));
      }
    }
    result.setProperty("@rid", iDocument.getIdentity());
    result.setProperty("@class", iDocument.getClassName());
    result.setProperty("@version", iDocument.getVersion());
    for (Map.Entry<String, ODocumentEntry> rawEntry : ODocumentInternal.rawEntries(iDocument)) {
      ODocumentEntry entry = rawEntry.getValue();
      if (entry.isChanged()) {
        result.setProperty(
            rawEntry.getKey(), convert(iDocument.getOriginalValue(rawEntry.getKey())));
      } else if (entry.isTrackedModified()) {
        if (entry.value instanceof ODocument && ((ODocument) entry.value).isEmbedded()) {
          result.setProperty(rawEntry.getKey(), calculateBefore((ODocument) entry.value, null));
        }
      }
    }
    return result;
  }

  private static Object convert(Object originalValue) {
    if (originalValue instanceof ORidBag) {
      Set result = new LinkedHashSet<>();
      ((ORidBag) originalValue).forEach(x -> result.add(x));
      return result;
    }
    return originalValue;
  }

  private static OResultInternal calculateAfter(
      ODocument iDocument, Set<String> projectionsToLoad) {
    OResultInternal result = new OResultInternal();
    for (String prop : iDocument.getPropertyNames()) {
      if (projectionsToLoad == null || projectionsToLoad.contains(prop)) {
        result.setProperty(prop, unboxRidbags(iDocument.getProperty(prop)));
      }
    }
    result.setProperty("@rid", iDocument.getIdentity());
    result.setProperty("@class", iDocument.getClassName());
    result.setProperty("@version", iDocument.getVersion() + 1);
    return result;
  }

  public static Object unboxRidbags(Object value) {
    // TODO move it to some helper class
    if (value instanceof ORidBag) {
      List<OIdentifiable> result = new ArrayList<>(((ORidBag) value).size());
      for (OIdentifiable oIdentifiable : (ORidBag) value) {
        result.add(oIdentifiable);
      }
      return result;
    }
    return value;
  }
}
