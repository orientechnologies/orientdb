package com.orientechnologies.orient.server.distributed.impl.lock;

import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionUniqueKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;

public class OLockManagerImpl implements OLockManager {

  private Map<OLockKey, Queue<OWaitingTracker>> locks = new ConcurrentHashMap<>();
  private boolean frozen;
  private OnFreezeAcquired frozenLock;

  public synchronized void freeze(OnFreezeAcquired frozenLock) {
    this.frozen = true;
    this.frozenLock = frozenLock;
    if (locks.isEmpty()) {
      this.frozenLock.acquired(() -> OLockManagerImpl.this.release());
    }
  }

  private synchronized void release() {
    this.frozen = false;
    this.frozenLock = null;
  }

  private void lock(OLockKey key, OWaitingTracker waitingTracker) {
    if (frozen) {
      throw new OOfflineNodeException("Node is offline");
    }
    Queue<OWaitingTracker> queue = locks.get(key);
    if (queue == null) {
      locks.put(key, new LinkedList<>());
    } else {
      queue.add(waitingTracker);
      waitingTracker.waitOne();
    }
  }

  private synchronized void unlock(OLockGuard guard) {
    Queue<OWaitingTracker> result = locks.get(guard.getKey());
    assert result != null : guard.getKey().toString();
    OWaitingTracker waiting = result.poll();
    if (waiting != null) {
      waiting.unlockOne();
    } else {
      locks.remove(guard.getKey());
    }
    if (frozen) {
      if (locks.isEmpty()) {
        this.frozenLock.acquired(() -> OLockManagerImpl.this.release());
      }
    }
  }

  public synchronized void lock(
      SortedSet<ORID> rids,
      SortedSet<OTransactionUniqueKey> indexKeys,
      OTransactionId transactionId,
      OnLocksAcquired acquired) {
    List<OLockGuard> guards = new ArrayList<>();
    OWaitingTracker waitingTracker = new OWaitingTracker(acquired);
    for (ORID rid : rids) {
      ORIDLockKey key = new ORIDLockKey(rid);
      lock(key, waitingTracker);
      guards.add(new OLockGuard(key));
    }

    for (OTransactionUniqueKey indexKey : indexKeys) {
      OIndexKeyLockKey key = new OIndexKeyLockKey(indexKey.getIndex(), indexKey.getKey());
      lock(key, waitingTracker);
      guards.add(new OLockGuard(key));
    }
    OTransactionIdLockKey key = new OTransactionIdLockKey(transactionId);
    lock(key, waitingTracker);
    guards.add(new OLockGuard(key));

    waitingTracker.setGuards(guards);
    waitingTracker.acquireIfNoWaiting();
  }

  public synchronized void unlock(List<OLockGuard> guards) {
    for (OLockGuard guard : guards) {
      unlock(guard);
    }
  }

  @Override
  public synchronized void lockResource(String name, OnLocksAcquired acquired) {
    OWaitingTracker waitingTracker = new OWaitingTracker(acquired);
    OResourceLockKey key = new OResourceLockKey(name);
    OLockGuard guard = new OLockGuard(key);
    lock(key, waitingTracker);
    waitingTracker.setGuards(Collections.singletonList(guard));
    waitingTracker.acquireIfNoWaiting();
  }
}
