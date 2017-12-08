package com.orientechnologies.common.concur.lock;

public interface OReadWriteLock {

  void acquireReadLock();

  void releaseReadLock();

  void releaseWriteLock();

  void acquireWriteLock();

}
