package com.orientechnologies.common.collection.closabledictionary;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OClosableDictionary<K, E extends OClosableItem> {
  private final Lock                                       closeLock = new ReentrantLock();
  private final ConcurrentHashMap<K, OClosableEntry<K, E>> data      = new ConcurrentHashMap<K, OClosableEntry<K, E>>();

  private final AtomicInteger lruCapacity = new AtomicInteger();


  public void add(K key, E item) {
    final OClosableEntry<K, E> closableEntry = new OClosableEntry<K, E>(item);
    final OClosableEntry<K, E> oldEntry = data.putIfAbsent(key, closableEntry);

    if (oldEntry != null) {
      throw new IllegalStateException("Item with key " + key + " already exists");
    }

    logAdd(closableEntry);
  }

  public E remove(K key) {
    final OClosableEntry<K, E> removed = data.remove(key);

    if (removed != null) {
      removed.makeRetired();
      logRemoved(removed);
      return removed.get();
    }

    return null;
  }

  public OClosableEntry<K, E> acquire(K key) {
    final OClosableEntry<K, E> entry = data.get(key);

    if (entry != null) {
      logAcquire(entry);
    }

    return null;
  }

  public void release(OClosableEntry<K, E> entry) {
    if (entry.get().isOpen()) {
      logRelease(entry);
    }
  }

  public OClosableItem get(K key) {
    final OClosableEntry<K, E> entry = data.get(key);
    if (entry != null)
      return entry.get();

    return null;
  }

  public void clear() {
    closeLock.lock();
    try {
      data.clear();
    } finally {
      closeLock.unlock();
    }
  }

  public Set<K> keySet() {
    return Collections.unmodifiableSet(data.keySet());
  }

  /**
   * Put the entry at the tail of LRU list if it is absent
   *
   * @param entry LRU entry
   */
  private void logAdd(OClosableEntry<K, E> entry) {

  }

  /**
   * Put entry at the tail of LRU list only if it is absent.
   *
   * @param entry LRU entry
   */
  private void logRelease(OClosableEntry<K, E> entry) {

  }

  /**
   * Put entry at the tail of LRU list
   *
   * @param entry LRU entry
   */
  private void logAcquire(OClosableEntry<K, E> entry) {

  }

  /**
   * Move the entry at the tail of LRU list
   *
   * @param entry LRU entry.
   */
  private void logLRUUpdate(OClosableEntry<K, E> entry) {

  }

  /**
   * Remove LRU entry from the LRU list.
   *
   * @param removed LRU entry.
   */
  private void logRemoved(OClosableEntry<K, E> removed) {

  }
}
