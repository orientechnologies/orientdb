package com.orientechnologies.common.concur.lock;

public interface OSimpleRWLockManager<T> {

  void acquireReadLock(T key, long timeout);

  void acquireWriteLock(T key, long timeout);

  void releaseReadLock(T key);

  void releaseWriteLock(T key);
}
