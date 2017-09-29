package com.orientechnologies.common.concur.lock;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OSimpleLockManagerImpl<T> implements OSimpleLockManager<T> {

  private final Lock              lock = new ReentrantLock();
  private final Map<T, Condition> map  = new HashMap<>();
  private final long timeout;

  public OSimpleLockManagerImpl(long timeout) {
    this.timeout = timeout;
  }

  @Override
  public void lock(T key) {

    lock.lock();
    try {
      Condition c;
      do {
        c = map.get(key);
        if (c != null) {
          try {
            if (!c.await(timeout, TimeUnit.MILLISECONDS)) {
              throw new OLockException(String.format("Time out acquire lock for resource: '%s' ", key));
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      } while (c != null);
      c = lock.newCondition();
      map.put(key, c);
    } finally {
      lock.unlock();
    }

  }

  @Override
  public void unlock(T key) {
    lock.lock();
    try {
      Condition c = map.remove(key);
      assert c != null;
      c.signalAll();
    } finally {
      lock.unlock();
    }
  }
}
