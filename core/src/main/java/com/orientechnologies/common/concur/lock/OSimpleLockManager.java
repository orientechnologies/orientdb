package com.orientechnologies.common.concur.lock;

public interface OSimpleLockManager<T> {

  void lock(T key);

  void unlock(T key);
}
