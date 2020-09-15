package com.orientechnologies.common.concur.lock;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.tx.OTransactionId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// Allows creating a promise and "upgrading" it to a lock which is a promise that cannot be broken!
public class OSimplePromiseManager {
  private final Lock lock = new ReentrantLock();
  private final Map<ORID, Promise> map = new ConcurrentHashMap<>();

  // todo: could use a timeout
  // todo: should I use Request ID instead of transaction ID?
  public void promise(ORID rid, int version, OTransactionId txId) {
    lock.lock();
    try {
      Promise p = map.get(rid);
      if (p != null) {
        throw new OLockException(
            String.format(
                "RID '%s' is promised with version %d to tx '%s'",
                rid, p.getVersion(), p.getTxId()));
      }
      map.put(rid, new Promise(version, txId));
    } finally {
      lock.unlock();
    }
  }

  public OTransactionId lock(ORID rid, int version, OTransactionId txId, boolean force) {
    lock.lock();
    try {
      Promise p = map.get(rid);
      if (p == null) {
        p = new Promise(version, txId);
        p.lock();
        map.put(rid, p);
      } else {
        if (p.isLocked()) {
          throw new OLockException(String.format("Cannot acquire lock for resource: '%s'", rid));
        }
        if (force) {
          // If there is a promise for an older version, must wait and retry later
          if (p.getVersion() < version) {
            throw new OLockException(String.format("Cannot acquire lock for resource: '%s'", rid));
          } else if (p.getVersion() > version) {
            // Ignore?
          } else {
            OTransactionId kickedOut = p.getTxId();
            p = new Promise(version, txId);
            p.lock();
            map.put(rid, p);
            return kickedOut;
          }
        } else {
          if (p.txId.equals(txId)) {
            p.lock();
          } else {
            throw new OLockException(
                String.format(
                    "Resource '%s' is promised with version %d to tx '%s'",
                    rid, p.getVersion(), p.getTxId()));
          }
        }
      }
    } finally {
      lock.unlock();
    }
    return null;
  }

  public void release(ORID rid, int version) {
    lock.lock();
    try {
      Promise p = map.get(rid);
      // assert p != null;
      // or safe to ignore?
      if (p.getVersion() == version) {
        map.remove(rid);
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

  public static class Promise {
    private final Integer version;
    private final OTransactionId txId;
    private boolean locked = false;

    public Promise(Integer version, OTransactionId txId) {
      this.version = version;
      this.txId = txId;
    }

    public Integer getVersion() {
      return version;
    }

    public OTransactionId getTxId() {
      return txId;
    }

    public void lock() {
      locked = true;
    }

    public boolean isLocked() {
      return locked;
    }
  }
}
