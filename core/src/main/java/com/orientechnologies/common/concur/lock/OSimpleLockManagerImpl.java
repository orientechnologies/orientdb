package com.orientechnologies.common.concur.lock;

import com.orientechnologies.common.exception.OException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OSimpleLockManagerImpl<T> implements OSimpleLockManager<T> {

  private final Lock              lock = new ReentrantLock();
  private final Map<T, Condition> map  = new ConcurrentHashMap<>();
  private final long              timeout;
  private       Set<T>            resetted;

  public OSimpleLockManagerImpl(long timeout) {
    this.timeout = timeout;
  }

  @Override
  public void lock(T key) {
    lock(key, timeout);
  }

  @Override
  public void lock(T key, long timeout) {

    lock.lock();
    try {
      try {

        Condition c;
        do {
          c = map.get(key);
          if (c != null) {
            if (timeout == 0) {
              c.await();
            } else {
              if (!c.await(timeout, TimeUnit.MILLISECONDS)) {
                throw new OLockException(String.format("Time out acquire lock for resource: '%s' ", key));
              }
            }
          }
        } while (c != null);
        c = lock.newCondition();
        map.put(key, c);
      } catch (InterruptedException e) {
        throw OException.wrapException(new OInterruptedException("Interrupted Lock"), e);
      }
    } finally {
      lock.unlock();
    }

  }

  @Override
  public void unlock(T key) {
    lock.lock();
    try {
      Condition c = map.remove(key);
      if (c == null) {
        if (!resetted.remove(key)) {
          assert c != null;
        }
      } else {
        c.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void reset() {
    lock.lock();
    try {
      resetted = new HashSet<>(map.keySet());
      map.entrySet().removeIf((c) -> {
        c.getValue().signalAll();
        return true;
      });
    } finally {
      lock.unlock();
    }
  }

  @Override
  public long getTimeout() {
    return timeout;
  }
}
