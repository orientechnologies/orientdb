package com.orientechnologies.common.concur.lock;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OSimpleReadWriteLock implements OReadWriteLock {
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  @Override
  public void acquireReadLock() {
    lock.readLock().lock();
  }

  @Override
  public void releaseReadLock() {
    lock.readLock().unlock();
  }

  @Override
  public void releaseWriteLock() {
    lock.writeLock().unlock();
  }

  @Override
  public void acquireWriteLock() {
    lock.writeLock().lock();
  }
}
