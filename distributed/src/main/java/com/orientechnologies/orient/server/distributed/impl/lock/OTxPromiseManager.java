package com.orientechnologies.orient.server.distributed.impl.lock;

import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.distributed.exception.ODistributedTxPromiseRequestIsOldException;
import com.orientechnologies.orient.server.distributed.exception.OTxPromiseException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** A promise manager keeps track of promises required for distributed transactions on a node. */
public class OTxPromiseManager<T> {
  private final Lock lock = new ReentrantLock();
  private final Map<T, OTxPromise<T>> map = new ConcurrentHashMap<>();

  /**
   * @param key
   * @param version
   * @param txId
   * @param force allows a tx to acquire the promise even if it is held by another tx. It must be
   *     used only in the second phase.
   * @return if using {@code force}, returns the ID of the tx that was previously holding the
   *     promise.
   */
  public OTransactionId promise(T key, int version, OTransactionId txId, boolean force) {
    lock.lock();
    try {
      OTxPromise<T> p = map.get(key);
      if (p == null) {
        map.put(key, new OTxPromise<>(key, version, txId));
        return null;
      }
      if (p.getTxId().equals(txId)) {
        return null;
      }
      if (!force) {
        throw new OTxPromiseException(
            String.format(
                "Cannot acquire promise for resource: '%s' v%d (existing version: %d)",
                key, version, p.getVersion()),
            version,
            p.getVersion());
      }
      // a phase2 force
      OTransactionId cancelledPromise = null;
      if (version < p.getVersion()) {
        // This promise will never happen since record versions never go back
        throw new ODistributedTxPromiseRequestIsOldException(
            String.format(
                "Cannot force acquire promise for resource: '%s' v%d (existing version: %d)",
                key, version, p.getVersion()));
      }
      if (version > p.getVersion()) {
        // If there is a promise for an older version, tx could be retried later
        throw new OTxPromiseException(
            String.format(
                "Cannot force acquire promise for resource: '%s' v%d (existing version: %d)",
                key, version, p.getVersion()),
            version,
            p.getVersion());
      }

      cancelledPromise = p.getTxId();
      map.remove(key);
      map.put(key, new OTxPromise<>(key, version, txId));
      return cancelledPromise;
    } finally {
      lock.unlock();
    }
  }

  public void release(T key, OTransactionId txId) {
    lock.lock();
    try {
      OTxPromise<T> p = map.get(key);
      if (p != null && p.getTxId().equals(txId)) {
        map.remove(key);
      }
    } finally {
      lock.unlock();
    }
  }

  public void reset() {
    lock.lock();
    try {
      map.clear();
    } finally {
      lock.unlock();
    }
  }

  public long size() {
    lock.lock();
    try {
      return map.size();
    } finally {
      lock.unlock();
    }
  }
}
