package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Concurrent implementation of {@link LRUList}.
 *
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class ConcurrentLRUList implements LRUList {
  private final AtomicReference<ListNode>       tailReference   = new AtomicReference<ListNode>();
  private ConcurrentHashMap<CacheKey, LRUEntry> cache           = new ConcurrentHashMap<CacheKey, LRUEntry>();
  private AtomicReference<ListNode>             headReference   = new AtomicReference<ListNode>();
  private ConcurrentLinkedQueue<ListNode>       trash           = new ConcurrentLinkedQueue<ListNode>();
  private AtomicBoolean                         purgeInProgress = new AtomicBoolean(false);

  @Override
  public OCacheEntry get(long fileId, long pageIndex) {
    purge();

    return cache.get(new CacheKey(fileId, pageIndex)).entry;
  }

  @Override
  public OCacheEntry remove(long fileId, long pageIndex) {
    CacheKey key = new CacheKey(fileId, pageIndex);
    final LRUEntry valueToRemove = cache.get(key);

    if (valueToRemove == null) {
      return null;
    } else {
      ListNode listNode = valueToRemove.listNode.getAndSet(null);
      if (listNode == null)
        return null;

      listNode.entry = null;
      trash.add(listNode);

      cache.remove(key);
      return valueToRemove.entry;
    }
  }

  @Override
  public void putToMRU(OCacheEntry cacheEntry) {
    final CacheKey key = new CacheKey(cacheEntry.getFileId(), cacheEntry.getPageIndex());
    LRUEntry value = new LRUEntry(key, cacheEntry);
    final LRUEntry existingValue = cache.putIfAbsent(key, value);
    if (existingValue != null) {
      existingValue.entry = cacheEntry;
      offer(existingValue, false);
    } else
      offer(value, true);
  }

  private void offer(LRUEntry lruEntry, boolean freshEntry) {
    ListNode tail = tailReference.get();
    if (tail == null || !lruEntry.equals(tail.entry)) {
      final ListNode newNode = new ListNode(lruEntry);

      final ListNode oldNode = lruEntry.listNode.getAndSet(newNode);
      if (oldNode != null) {
        oldNode.entry = null;
        trash.add(oldNode);
      } else {
        if (!freshEntry) {
          // Some other thread removed this entry from cache concurrently.
          return;
        }
      }

      do {
        tail = tailReference.get();
        newNode.prev = tail;
      } while (!tailReference.compareAndSet(tail, newNode));

      if (tail != null)
        tail.next = newNode;
      else {
        boolean casResult = headReference.compareAndSet(null, newNode);
        assert casResult;
      }
    }
  }

  @Override
  public OCacheEntry removeLRU() {
    ListNode head = headReference.get();
    ListNode current = head;

    if (current == null)
      return null;

    do {
      while (currentEntryDeletedOrInUse(current)) {
        ListNode next = current.next;
        if (next == null)
          return null;

        current = next;
      }
      headReference.compareAndSet(head, current);

      final LRUEntry oldEntry = current.entry;
      if (oldEntry == null || !oldEntry.listNode.compareAndSet(current, null)) {
        continue;
      }
      current.entry = null;
      trash.add(current);

      cache.remove(oldEntry.key);
      purge();
      return oldEntry.entry;

    } while (true);
  }

  private boolean currentEntryDeletedOrInUse(ListNode current) {
    LRUEntry entry = current.entry;
    return entry == null || isInUse(entry.entry);
  }

  private void purge() {
    if (purgeInProgress.compareAndSet(false, true)) {

      purgeSomeFromTrash();

      purgeInProgress.set(false);
    }
  }

  private void purgeSomeFromTrash() {
    for (int i = 0; i < 15; i++) {
      ListNode node = trash.poll();
      if (node == null)
        return;

      if (tailReference.get() == node) {
        trash.add(node);
        return;
      }

      if (node.next != null)
        node.next.prev = node.prev;

      if (node.prev == null) {
        headReference.compareAndSet(node, node.next);
      } else {
        node.prev.next = node.next;
      }
    }
  }

  @Override
  public void clear() {
    cache.clear();
    headReference.set(null);
    tailReference.set(null);
  }

  @Override
  public boolean contains(long fileId, long filePosition) {
    return cache.containsKey(new CacheKey(fileId, filePosition));
  }

  @Override
  public int size() {
    return cache.size();
  }

  private boolean isInUse(OCacheEntry entry) {
    return entry != null && entry.usagesCount != 0;
  }

  @Override
  public Iterator<OCacheEntry> iterator() {
    return new OCacheEntryIterator(tailReference.get());
  }

  private class OCacheEntryIterator implements Iterator<OCacheEntry> {

    private ListNode current;

    public OCacheEntryIterator(ListNode start) {
      current = start;
      while (current != null && current.entry == null)
        current = current.prev;
    }

    @Override
    public boolean hasNext() {
      return current != null && current.entry != null;
    }

    @Override
    public OCacheEntry next() {
      final OCacheEntry entry = current.entry.entry;

      do
        current = current.prev;
      while (current != null && current.entry == null);

      return entry;
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
    private final AtomicReference<ListNode> listNode = new AtomicReference<ListNode>();
    private final CacheKey                  key;
    private OCacheEntry                     entry;

    private LRUEntry(CacheKey key, OCacheEntry entry) {
      this.key = key;
      this.entry = entry;
    }
  }

  private static class ListNode {
    private volatile LRUEntry entry;
    private volatile ListNode prev;
    private volatile ListNode next;

    private ListNode(LRUEntry key) {
      this.entry = key;
    }
  }
}
