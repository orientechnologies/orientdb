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
  private final Map<T, Promise<T>> map = new ConcurrentHashMap<>();
  private final Map<T, Condition> conditions = new ConcurrentHashMap<>();
  private final long timeout; // in milliseconds
  private Set<T>     reset;

  public OTxPromiseManager(long timeout) {
    this.timeout = timeout;
  }

  // todo: should I use Request ID instead of transaction ID?
  public OTransactionId promise(T key, int version, OTransactionId txId, boolean force) {
    lock.lock();
    try {
      Promise<T> p = map.get(key);
      if (p == null) {
        map.put(key, new Promise<>(key, version, txId));
        conditions.put(key, lock.newCondition());
        return null;
      }
      if (p.getTxId().equals(txId)) {
        return null;
      }
      if (force) {
        OTransactionId cancelledPromise = null;
        // If there is a promise for an older version, must wait and retry later
        if (p.getVersion() < version) {
          // todo: use different exceptions
          throw new OLockException(String.format("Cannot acquire lock for resource: '%s'", key));
        } else if (p.getVersion() > version) {
          // Ignore?
        } else {
          cancelledPromise = p.getTxId();
          map.remove(key);
          Condition c = conditions.remove(key);
          assert c != null; // todo: is this necessary?
          c.notifyAll();
          map.put(key, new Promise<>(key, version, txId));
          conditions.put(key, lock.newCondition());
        }
        return cancelledPromise;
      }
      // wait until promise is available or timeout
      try {
        while (p != null) {
          Condition c = conditions.get(key);
          if (c != null && !c.await(timeout, TimeUnit.MILLISECONDS)) {
            throw new OLockException(
                String.format(
                    "Resource '%s' is promised with version %d to tx '%s'",
                    key, p.getVersion(), p.getTxId()));
          }
          p = map.get(key);
        }
        map.put(key, new Promise<>(key, version, txId));
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

  public void release(T key, int version) { // todo: require txId? version needed?
    lock.lock();
    try {
      Promise<T> p = map.get(key);
      if (p == null) {
        // The only way a release will not see the promise again is if there was a reset meanwhile.
        if (!reset.remove(key)) {
          assert p != null; // todo: use an exception with better message?
        }
      } else {
        // todo: assert version and txId?
        if (p.getVersion() == version) {
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
}
