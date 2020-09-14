package com.orientechnologies.common.concur.lock;

import com.orientechnologies.orient.core.id.ORID;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OSimplePromiseManager {
  private final Lock lock = new ReentrantLock();
  private final Map<ORID, Integer> map = new ConcurrentHashMap<>();

  public void promise(ORID rid, int version) {
    lock.lock();
    try {
      Integer v = map.get(rid);
      if (v != null) {
        throw new OLockException(String.format("RID '%s' is promised with version %d", rid, v));
      }
      map.put(rid, version);
    } finally {
      lock.unlock();
    }
  }

  public void release(ORID rid, int version) {
    lock.lock();
    try {
      Integer v = map.get(rid);
      if (v == null || v != version) {
        // safe to ignore?
      } else {
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
}
