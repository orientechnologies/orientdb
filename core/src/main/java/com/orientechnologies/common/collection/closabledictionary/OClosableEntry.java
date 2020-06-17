package com.orientechnologies.common.collection.closabledictionary;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Internal presentation of entry inside of {@link OClosableLinkedContainer}
 *
 * @param <K> Key type.
 * @param <V> Value type.
 */
public class OClosableEntry<K, V extends OClosableItem> {
  /** Constant for open state of entries state machine. */
  private static final long STATUS_OPEN = 1;

  /** Constant for closed state of entries state machine. */
  private static final long STATUS_CLOSED = 2;

  /** Constant for retired state of entries state machine. */
  private static final long STATUS_RETIRED = 4;

  /** Constant for dead state of entry state machine. */
  private static final long STATUS_DEAD = 5;

  /**
   * Because entry may be acquired by several threads acquired status instead of single constant is
   * presented as number of acquires and is stored in 4 most significant bytes of {@link #state}
   * field.
   */
  private static final long ACQUIRED_OFFSET = 32;

  public static boolean isOpen(long state) {
    return state == STATUS_OPEN;
  }

  private OClosableEntry<K, V> next;

  private OClosableEntry<K, V> prev;

  public OClosableEntry<K, V> getNext() {
    return next;
  }

  public void setNext(OClosableEntry<K, V> next) {
    this.next = next;
  }

  public OClosableEntry<K, V> getPrev() {
    return prev;
  }

  public void setPrev(OClosableEntry<K, V> prev) {
    this.prev = prev;
  }

  private final V item;

  /** Current state of state machine */
  private volatile long state = STATUS_OPEN;

  private final Lock stateLock = new ReentrantLock();

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
    if (s != STATUS_CLOSED) throw new IllegalStateException();

    final long acquiredState = 1L << ACQUIRED_OFFSET;
    item.open();

    state = acquiredState;
  }

  void makeAcquiredFromOpen() {
    if (state != STATUS_OPEN) throw new IllegalStateException();

    state = 1L << ACQUIRED_OFFSET;
  }

  void releaseAcquired() {
    stateLock.lock();
    try {
      long acquireCount = state >>> ACQUIRED_OFFSET;

      if (acquireCount < 1) throw new IllegalStateException("Amount of acquires less than one");

      acquireCount--;

      if (acquireCount < 1) state = STATUS_OPEN;
      else state = acquireCount << ACQUIRED_OFFSET;
    } finally {
      stateLock.unlock();
    }
  }

  void incrementAcquired() {
    long acquireCount = state >>> ACQUIRED_OFFSET;

    if (acquireCount < 1) throw new IllegalStateException();

    acquireCount++;
    state = acquireCount << ACQUIRED_OFFSET;
  }

  long makeRetired() {
    long oldSate = state;

    stateLock.lock();
    try {
      state = STATUS_RETIRED;
    } finally {
      stateLock.unlock();
    }

    return oldSate;
  }

  void makeDead() {
    stateLock.lock();
    try {
      state = STATUS_DEAD;
    } finally {
      stateLock.unlock();
    }
  }

  boolean makeClosed() {
    stateLock.lock();
    try {
      if (state == STATUS_CLOSED) return true;

      if (state != STATUS_OPEN) return false;

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
