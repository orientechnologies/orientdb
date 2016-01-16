/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.storage.cache.local;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;

import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Concurrent implementation of {@link LRUList}.
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class ConcurrentLRUList implements LRUList {

  private static boolean                              assertionsEnabled;

  static {
    assert assertionsEnabled = true;
  }

  private final ConcurrentHashMap<CacheKey, LRUEntry> cache           = new ConcurrentHashMap<CacheKey, LRUEntry>();
  private final ListNode                              headReference   = new ListNode(null, true);
  private final AtomicReference<ListNode>             tailReference   = new AtomicReference<ListNode>(headReference);

  private final ConcurrentLinkedQueue<ListNode>       trash           = new ConcurrentLinkedQueue<ListNode>();

  private final int                                   minTrashSize    = Runtime.getRuntime().availableProcessors() * 4;
  private final AtomicBoolean                         purgeInProgress = new AtomicBoolean();
  private final AtomicInteger                         trashSize       = new AtomicInteger();

  public ConcurrentLRUList() {
  }

  @Override
  public OCacheEntry get(long fileId, long pageIndex) {
    final LRUEntry lruEntry = cache.get(new CacheKey(fileId, pageIndex));

    purge();

    if (lruEntry == null)
      return null;

    return lruEntry.entry;
  }

  @Override
  public OCacheEntry remove(long fileId, long pageIndex) {
    CacheKey key = new CacheKey(fileId, pageIndex);
    final LRUEntry valueToRemove = cache.remove(key);

    if (valueToRemove == null)
      return null;

    valueToRemove.removeLock.writeLock().lock();
    try {
      valueToRemove.removed = true;
      ListNode node = valueToRemove.listNode.get();
      valueToRemove.listNode.lazySet(null);

      if (node != null)
        addToTrash(node);
    } finally {
      valueToRemove.removeLock.writeLock().unlock();
    }

    purge();

    return valueToRemove.entry;
  }

  @Override
  public void putToMRU(OCacheEntry cacheEntry) {
    final CacheKey key = new CacheKey(cacheEntry.getFileId(), cacheEntry.getPageIndex());
    LRUEntry value = new LRUEntry(key, cacheEntry);
    final LRUEntry existingValue = cache.putIfAbsent(key, value);

    if (existingValue != null) {
      existingValue.entry = cacheEntry;
      offer(existingValue);
    } else
      offer(value);

    purge();
  }

  private void offer(LRUEntry lruEntry) {
    lruEntry.removeLock.readLock().lock();
    try {
      if (lruEntry.removed)
        return;

      ListNode tail = tailReference.get();

      if (!lruEntry.equals(tail.entry)) {
        final ListNode oldNode = lruEntry.listNode.get();

        ListNode newNode = new ListNode(lruEntry, false);
        if (lruEntry.listNode.compareAndSet(oldNode, newNode)) {

          while (true) {
            newNode.previous.set(tail);

            if (tail.next.compareAndSet(null, newNode)) {
              tailReference.compareAndSet(tail, newNode);
              break;
            }

            tail = tailReference.get();
          }

          if (oldNode != null)
            addToTrash(oldNode);
        }
      }

    } finally {
      lruEntry.removeLock.readLock().unlock();
    }
  }

  @Override
  public OCacheEntry removeLRU() {
    ListNode current = headReference;

    boolean removed = false;

    LRUEntry currentEntry = null;
    int inUseCounter = 0;
    do {
      while (current.isDummy || (currentEntry = current.entry) == null || (isInUse(currentEntry.entry))) {
        if (currentEntry != null && isInUse(currentEntry.entry))
          inUseCounter++;

        ListNode next = current.next.get();

        if (next == null) {
          if (cache.size() == inUseCounter)
            return null;

          current = headReference;
          inUseCounter = 0;
          continue;
        }

        current = next;
      }

      if (cache.remove(currentEntry.key, currentEntry)) {
        currentEntry.removeLock.writeLock().lock();
        try {
          currentEntry.removed = true;
          ListNode node = currentEntry.listNode.get();

          currentEntry.listNode.lazySet(null);
          addToTrash(node);
          removed = true;
        } finally {
          currentEntry.removeLock.writeLock().unlock();
        }
      } else {
        current = headReference;
        inUseCounter = 0;
      }

    } while (!removed);

    purge();

    return currentEntry.entry;
  }

  @Override
  public OCacheEntry getLRU() {
    ListNode current = headReference;

    LRUEntry currentEntry = null;
    int inUseCounter = 0;

    while (current.isDummy || (currentEntry = current.entry) == null || (isInUse(currentEntry.entry))) {
      if (currentEntry != null && isInUse(currentEntry.entry))
        inUseCounter++;

      ListNode next = current.next.get();

      if (next == null) {
        if (cache.size() == inUseCounter)
          return null;

        current = headReference;
        inUseCounter = 0;
        continue;
      }

      current = next;
    }

    purge();

    return currentEntry.entry;
  }

  private void purge() {
    if (purgeInProgress.compareAndSet(false, true)) {
      purgeSomeFromTrash();

      purgeInProgress.set(false);
    }
  }

  private void purgeSomeFromTrash() {
    int additionalSize = 0;

    while (trashSize.get() >= minTrashSize + additionalSize) {

      final ListNode node = trash.poll();
      trashSize.decrementAndGet();

      if (node == null)
        return;

      if (node.next.get() == null) {
        trash.add(node);
        trashSize.incrementAndGet();
        additionalSize++;
        continue;
      }

      final ListNode previous = node.previous.get();
      final ListNode next = node.next.get();

      node.previous.lazySet(null);

      assert previous.next.get() == node;
      assert next == null || next.previous.get() == node;

      if (assertionsEnabled) {
        boolean success = previous.next.compareAndSet(node, next);
        assert success;
      } else
        previous.next.set(next);

      if (next != null)
        next.previous.set(previous);
    }
  }

  @Override
  public void clear() {
    cache.clear();

    headReference.next.set(null);
    tailReference.set(headReference);

    trash.clear();
    trashSize.set(0);
  }

  @Override
  public boolean contains(long fileId, long filePosition) {
    return cache.containsKey(new CacheKey(fileId, filePosition));
  }

  private void addToTrash(ListNode node) {
    node.entry = null;

    trash.add(node);
    trashSize.incrementAndGet();
  }

  @Override
  public int size() {
    return cache.size();
  }

  private boolean isInUse(OCacheEntry entry) {
    return entry != null && entry.getUsagesCount() != 0;
  }

  @Override
  public Iterator<OCacheEntry> iterator() {
    return new OCacheEntryIterator(tailReference.get());
  }

  private static class OCacheEntryIterator implements Iterator<OCacheEntry> {

    private ListNode current;

    public OCacheEntryIterator(ListNode start) {
      current = start;
      while (current != null && current.entry == null)
        current = current.previous.get();
    }

    @Override
    public boolean hasNext() {
      return current != null && current.entry != null;
    }

    @Override
    public OCacheEntry next() {
      final OCacheEntry entry = current.entry.entry;

      do
        current = current.previous.get();
      while (current != null && current.entry == null);

      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static class CacheKey {
    private final long fileId;
    private final long pageIndex;

    private CacheKey(long fileId, long pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      CacheKey that = (CacheKey) o;

      if (fileId != that.fileId)
        return false;
      if (pageIndex != that.pageIndex)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = (int) (fileId ^ (fileId >>> 32));
      result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
      return result;
    }
  }

  private static class LRUEntry {
    private final AtomicReference<ListNode> listNode   = new AtomicReference<ListNode>();
    private final CacheKey                  key;
    private volatile OCacheEntry            entry;

    private boolean                         removed    = false;
    private final ReadWriteLock             removeLock = new ReentrantReadWriteLock();

    private LRUEntry(CacheKey key, OCacheEntry entry) {
      this.key = key;
      this.entry = entry;
    }
  }

  private static class ListNode {
    private volatile LRUEntry               entry;
    private final AtomicReference<ListNode> next     = new AtomicReference<ListNode>();
    private final AtomicReference<ListNode> previous = new AtomicReference<ListNode>();

    private final boolean                   isDummy;

    private ListNode(LRUEntry key, boolean isDummy) {
      this.entry = key;
      this.isDummy = isDummy;
    }
  }
}
