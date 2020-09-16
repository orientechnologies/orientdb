package com.orientechnologies.common.concur.lock;

import com.orientechnologies.orient.core.tx.OTransactionId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// Allows creating a promise and "upgrading" it to a lock which is a promise that cannot be broken!
public class OSimplePromiseManager<T> {
  private final Lock lock = new ReentrantLock();
  private final Map<T, Promise<T>> map = new ConcurrentHashMap<>();

  // todo: could use a timeout
  // todo: should I use Request ID instead of transaction ID?
  public void promise(T key, int version, OTransactionId txId) {
    lock.lock();
    try {
      Promise<T> p = map.get(key);
      if (p != null) {
        throw new OLockException(
            String.format(
                "Resource '%s' is promised with version %d to tx '%s'",
                key, p.getVersion(), p.getTxId()));
      }
      map.put(key, new Promise<>(key, version, txId));
    } finally {
      lock.unlock();
    }
  }

  public OTransactionId lock(T key, int version, OTransactionId txId, boolean force) {
    lock.lock();
    try {
      Promise<T> p = map.get(key);
      if (p == null) {
        p = new Promise<>(key, version, txId);
        p.lock();
        map.put(key, p);
      } else {
        if (p.isLocked()) {
          throw new OLockException(String.format("Cannot acquire lock for resource: '%s'", key));
        }
        if (force) {
          // If there is a promise for an older version, must wait and retry later
          if (p.getVersion() < version) {
            throw new OLockException(String.format("Cannot acquire lock for resource: '%s'", key));
          } else if (p.getVersion() > version) {
            // Ignore?
          } else {
            OTransactionId kickedOut = p.getTxId();
            p = new Promise<>(key, version, txId);
            p.lock();
            map.put(key, p);
            return kickedOut;
          }
        } else {
          if (p.getTxId().equals(txId)) {
            p.lock();
          } else {
            throw new OLockException(
                String.format(
                    "Resource '%s' is promised with version %d to tx '%s'",
                    key, p.getVersion(), p.getTxId()));
          }
        }
      }
    } finally {
      lock.unlock();
    }
    return null;
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
