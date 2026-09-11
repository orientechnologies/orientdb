package com.orientechnologies.orient.core.query.live;

import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public class OLiveQueryOps implements OCloseable {

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

  public void enqueue(OLiveQueryOp item) {
    queue.offer(item);
  }

  public Integer subscribe(Integer id, OLiveQueryListenerV2 iListener) {
    synchronized (threadLock) {
      if (!queueThread.isAlive()) {
        queueThread = queueThread.clone();
        queueThread.start();
      }
    }
    subscribers.put(id, iListener);
    return id;
  }

  public void unsubscribe(Integer id) {
    synchronized (threadLock) {
      OLiveQueryListenerV2 res = subscribers.remove(id);
      if (res != null) {
        res.onLiveResultEnd();
      }
    }
  }

  public void enqueueForDb(ODatabaseDocument database) {
    if (pendingOps.isEmpty()) {
      return;
    }
    List<OLiveQueryOp> list;
    synchronized (pendingOps) {
      list = pendingOps.remove(database);
    }
    // TODO sync
    if (list != null) {
      for (OLiveQueryOp item : list) {
        item.setOriginalDoc(item.getOriginalDoc().copy());
        enqueue(item);
      }
    }
  }

  public boolean hasListeners() {
    return !subscribers.isEmpty();
  }

  public void removeForDb(ODatabaseDocument database) {
    synchronized (pendingOps) {
      pendingOps.remove(database);
    }
  }

  public void addOp(OLiveQueryOp result, ODatabaseDocument db) {
    synchronized (pendingOps) {
      List<OLiveQueryOp> list = pendingOps.get(db);
      if (list == null) {
        list = new ArrayList<>();
        pendingOps.put(db, list);
      }
      if (result.getType() == ORecordOperation.UPDATED) {
        OLiveQueryOp prev = prevousUpdate(list, result.getOriginalDoc());
        if (prev == null) {
          list.add(result);
        } else {
          prev.setAfter(result.getAfter());
        }
      } else {
        list.add(result);
      }
    }
  }

  private static OLiveQueryOp prevousUpdate(List<OLiveQueryOp> list, ODocument doc) {
    for (OLiveQueryOp oLiveQueryOp : list) {
      if (oLiveQueryOp.getOriginalDoc() == doc) {
        return oLiveQueryOp;
      }
    }
    return null;
  }
}
