package com.orientechnologies.common.concur.lock;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OSimpleLockManagerImpl<T> implements OSimpleLockManager<T> {

  private final Lock              lock = new ReentrantLock();
  private final Map<T, Condition> map  = new HashMap<>();

  @Override
  public void lock(T key) {

    lock.lock();
    try {
      Condition c;
      do {
        c = map.get(key);
        if (c != null) {
          try {
            c.await();
          } catch (InterruptedException e) {
            Thread.interrupted();
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
