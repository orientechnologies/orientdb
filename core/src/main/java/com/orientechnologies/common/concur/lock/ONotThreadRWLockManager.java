package com.orientechnologies.common.concur.lock;

import com.orientechnologies.common.exception.OException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ONotThreadRWLockManager<T> implements OSimpleRWLockManager<T> {

  private static class LockGuard {
    private volatile int count;
    private final Condition condition;
    private final boolean shared;

    public LockGuard(int count, Condition condition, boolean shared) {
      this.count = count;
      this.condition = condition;
      this.shared = shared;
    }
  }

  private final Lock lock = new ReentrantLock();
  private final Map<T, LockGuard> map = new ConcurrentHashMap<>();

  public ONotThreadRWLockManager() {}

  public void lock(T key, boolean shared, long timeout) {

    lock.lock();
    try {
      try {

        LockGuard c;
        do {
          c = map.get(key);
          if (c != null) {
            if (c.shared && shared) {
              c.count++;
              return;
            } else {
              if (timeout == 0) {
                c.condition.await();
              } else {
                if (!c.condition.await(timeout, TimeUnit.MILLISECONDS)) {
                  throw new OLockException(
                      String.format("Time out acquire lock for resource: '%s' ", key));
                }
              }
            }
          }
        } while (c != null);
        c = new LockGuard(1, lock.newCondition(), shared);
        map.put(key, c);
      } catch (InterruptedException e) {
        throw OException.wrapException(new OInterruptedException("Interrupted Lock"), e);
      }
    } finally {
      lock.unlock();
    }
  }

  public void unlock(T key, boolean shared) {
    lock.lock();
    try {
      LockGuard c = map.get(key);
      assert c != null;
      if (c.shared != shared) {
        throw new OLockException("Impossible to release a not acquired lock");
      }
      c.count--;
      if (c.count == 0) {
        map.remove(key);
        c.condition.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void acquireReadLock(T key, long timeout) {
    lock(key, true, timeout);
  }

  @Override
  public void acquireWriteLock(T key, long timeout) {
    lock(key, false, timeout);
  }

  @Override
  public void releaseReadLock(T key) {
    unlock(key, true);
  }

  @Override
  public void releaseWriteLock(T key) {
    unlock(key, false);
  }
}
