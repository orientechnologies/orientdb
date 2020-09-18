package com.orientechnologies.common.concur.lock;

import com.orientechnologies.orient.core.tx.OTransactionId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OTxPromiseManager<T> {
  private final Lock lock = new ReentrantLock();
  private final Map<T, Promise<T>> map = new ConcurrentHashMap<>();

  // todo: could use a timeout
  // todo: should I use Request ID instead of transaction ID?
  public OTransactionId promise(T key, int version, OTransactionId txId, boolean force) {
    lock.lock();
    try {
      Promise<T> p = map.get(key);
      if (p == null) {
        map.put(key, new Promise<>(key, version, txId));
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
        }
        return cancelledPromise;
      } else {
        // todo: use timeout and wait?
        throw new OLockException(
            String.format(
                "Resource '%s' is promised with version %d to tx '%s'",
                key, p.getVersion(), p.getTxId()));
      }
    } finally {
      lock.unlock();
    }
  }

  public void release(T key, int version) { // todo: require txId?
    lock.lock();
    try {
      Promise<T> p = map.get(key);
      // assert p != null;
      // or safe to ignore?
      if (p != null && p.getVersion() == version) {
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

  public long getTimeout() {
    return -1;
  }
}
