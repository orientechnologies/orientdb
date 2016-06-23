package com.orientechnologies.common.collection.closabledictionary;

import javax.annotation.concurrent.GuardedBy;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OClosableEntry<K, V> {
  private static final long STATUS_OPEN    = 1;
  private static final long STATUS_CLOSED  = 2;
  private static final long STATUS_RETIRED = 4;
  private static final long STATUS_DEAD    = 5;

  private static final long ACQUIRED_OFFSET = 32;

  @GuardedBy("lruLock")
  private OClosableEntry<K, V> next;

  @GuardedBy("lruLock")
  private OClosableEntry<K, V> prev;

  @GuardedBy("lruLock")
  public OClosableEntry<K, V> getNext() {
    return next;
  }

  @GuardedBy("lruLock")
  public void setNext(OClosableEntry<K, V> next) {
    this.next = next;
  }

  @GuardedBy("lruLock")
  public OClosableEntry<K, V> getPrev() {
    return prev;
  }

  @GuardedBy("lruLock")
  public void setPrev(OClosableEntry<K, V> prev) {
    this.prev = prev;
  }

  private final V item;

  private volatile long state     = STATUS_OPEN;
  private final    Lock stateLock = new ReentrantLock();

  public OClosableEntry(V item) {
    this.item = item;
  }

  public V get() {
    return item;
  }

  public void acquireStateLock() {
    stateLock.lock();
  }

  public void releaseStateLock() {
    stateLock.unlock();
  }

  void makeAcquiredFromClosed(OClosableItem item) {
    final long s = state;
    if (s != STATUS_CLOSED)
      throw new IllegalStateException();

    final long acquiredState = 1L << ACQUIRED_OFFSET;
    item.open();

    state = acquiredState;
  }

  void makeAcquiredFromOpen() {
    if (state != STATUS_OPEN)
      throw new IllegalStateException();

    state = 1L << ACQUIRED_OFFSET;
  }

  void releaseAcquired() {
    stateLock.lock();
    try {
      long acquireCount = state >>> ACQUIRED_OFFSET;

      if (acquireCount < 1)
        throw new IllegalStateException("Amount of acquires less than one");

      acquireCount--;

      if (acquireCount < 1)
        state = STATUS_OPEN;
      else
        state = acquireCount << ACQUIRED_OFFSET;
    } finally {
      stateLock.unlock();
    }
  }

  void incrementAcquired() {
    long acquireCount = state >>> ACQUIRED_OFFSET;

    if (acquireCount < 1)
      throw new IllegalStateException();

    acquireCount++;
    state = acquireCount << ACQUIRED_OFFSET;
  }

  void makeRetired() {
    stateLock.lock();
    try {
      state = STATUS_RETIRED;
    } finally {
      stateLock.unlock();
    }
  }

  void makeDead() {
    stateLock.lock();
    try {
      state = STATUS_DEAD;
    } finally {
      stateLock.unlock();
    }
  }

  boolean makeClosed(OClosableItem item) {
    stateLock.lock();
    try {
      if (state != STATUS_OPEN)
        return false;

      item.close();
      state = STATUS_CLOSED;
    } finally {
      stateLock.unlock();
    }

    return true;
  }

  boolean isClosed() {
    return state == STATUS_CLOSED;
  }

  boolean isRetired() {
    return state == STATUS_RETIRED;
  }

  boolean isDead() {
    return state == STATUS_DEAD;
  }

  boolean isOpen() {
    return state == STATUS_OPEN;
  }
}
