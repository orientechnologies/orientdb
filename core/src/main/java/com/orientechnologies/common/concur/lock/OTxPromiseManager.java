package com.orientechnologies.common.concur.lock;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.tx.OTransactionId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OTxPromiseManager<T> {
  private final Lock lock = new ReentrantLock();
  private final Map<T, OTxPromise<T>> map = new ConcurrentHashMap<>();
  private final Map<T, Condition> conditions = new ConcurrentHashMap<>();
  private final long timeout; // in milliseconds
  private Set<T> reset;

  public OTxPromiseManager(long timeout) {
    this.timeout = timeout;
  }

  public OTransactionId promise(T key, int version, OTransactionId txId, boolean force) {
    lock.lock();
    try {
      OTxPromise<T> p = map.get(key);
      if (p == null) {
        map.put(key, new OTxPromise<>(key, version, txId));
        conditions.put(key, lock.newCondition());
        return null;
      }
      if (p.getTxId().equals(txId)) {
        return null;
      }
      if (force) {
        OTransactionId cancelledPromise = null;
        if (p.getVersion() != version) {
          throw new OTxPromiseException(
              String.format("Cannot acquire promise for resource: '%s'", key),
              version,
              p.getVersion());
        } else {
          cancelledPromise = p.getTxId();
          map.remove(key);
          Condition c = conditions.remove(key);
          if (c != null) {
            c.notifyAll();
          }
          map.put(key, new OTxPromise<>(key, version, txId));
          conditions.put(key, lock.newCondition());
        }
        return cancelledPromise;
      }
      // wait until promise is available or timeout
      try {
        while (p != null) {
          Condition c = conditions.get(key);
          if (c != null && !c.await(timeout, TimeUnit.MILLISECONDS)) {
            throw new OTxPromiseException(
                String.format("Timed out waiting to acquire promise for resource '%s'", key),
                version,
                p.getVersion());
          }
          p = map.get(key);
        }
        map.put(key, new OTxPromise<>(key, version, txId));
        conditions.put(key, lock.newCondition());
        return null;
      } catch (InterruptedException e) {
        throw OException.wrapException(
            new OInterruptedException("Interrupted waiting for promise"), e);
      }
    } finally {
      lock.unlock();
    }
  }

  // TODO(PS): does release need a version?
  public void release(T key, int version, OTransactionId txId) {
    lock.lock();
    try {
      OTxPromise<T> p = map.get(key);
      if (p == null) {
        // todo(PS): can release happen norma
        // The only way a release will not see the promise again is if there was a reset meanwhile.
        if (!reset.remove(key)) {
          assert p != null; // todo: use an exception with better message?
        }
      } else {
        if (p.getTxId().equals(txId) && p.getVersion() == version) {
          map.remove(key);
          Condition c = conditions.remove(key);
          c.notifyAll();
        }
      }
    } finally {
      lock.unlock();
    }
  }

  public void reset() {
    lock.lock();
    try {
      reset = new HashSet<>(map.keySet());
      map.entrySet()
          .removeIf(
              (c) -> {
                conditions.remove(c.getKey()).notifyAll();
                return true;
              });
    } finally {
      lock.unlock();
    }
  }

  public long getTimeout() {
    return timeout;
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
