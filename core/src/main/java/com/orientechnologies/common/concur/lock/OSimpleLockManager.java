package com.orientechnologies.common.concur.lock;

public interface OSimpleLockManager<T> {

  void lock(T key);

  void lock(T key, long timeout);

  void unlock(T key);

  void reset();

  long getTimeout();
}
