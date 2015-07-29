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

import com.orientechnologies.common.hash.OMurmurHash3;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Andrey Lomakin
 * @since 25.02.13
 */
class HashLRUList implements LRUList {
  private static final int SEED = 362498820;

  private LRUEntry         head;
  private LRUEntry         tail;

  private int              nextThreshold;
  private int              size;

  private LRUEntry         entries[];

  public HashLRUList() {
    entries = new LRUEntry[1024];
    nextThreshold = (int) (entries.length * 0.75);
  }

  @Override
  public OCacheEntry get(long fileId, long pageIndex) {
    long hashCode = hashCode(fileId, pageIndex);
    int index = index(hashCode);

    LRUEntry lruEntry = entries[index];

    while (lruEntry != null
        && (lruEntry.hashCode != hashCode || lruEntry.cacheEntry.getPageIndex() != pageIndex || lruEntry.cacheEntry.getFileId() != fileId))
      lruEntry = lruEntry.next;

    if (lruEntry == null)
      return null;

    return lruEntry.cacheEntry;
  }

  @Override
  public OCacheEntry remove(long fileId, long pageIndex) {
    long hashCode = hashCode(fileId, pageIndex);
    int index = index(hashCode);

    LRUEntry lruEntry = entries[index];

    LRUEntry prevEntry = null;
    while (lruEntry != null
        && (lruEntry.hashCode != hashCode || lruEntry.cacheEntry.getFileId() != fileId || lruEntry.cacheEntry.getPageIndex() != pageIndex)) {
      prevEntry = lruEntry;
      lruEntry = lruEntry.next;
    }

    if (lruEntry == null)
      return null;

    assert tail == null || tail.before != tail;
    assert tail == null || tail.after == null;

    removeFromLRUList(lruEntry);

    if (prevEntry == null)
      entries[index] = lruEntry.next;
    else
      prevEntry.next = lruEntry.next;

    assert tail == null || tail.before != tail;
    assert tail == null || tail.after == null;

    size--;

    return lruEntry.cacheEntry;
  }

  private void removeFromLRUList(LRUEntry lruEntry) {
    LRUEntry before = lruEntry.before;
    LRUEntry after = lruEntry.after;

    if (before != null)
      before.after = after;
    if (after != null)
      after.before = before;

    if (lruEntry == head)
      head = lruEntry.after;
    if (lruEntry == tail)
      tail = lruEntry.before;
  }

  @Override
  public void putToMRU(OCacheEntry cacheEntry) {
    final long fileId = cacheEntry.getFileId();
    final long pageIndex = cacheEntry.getPageIndex();

    long hashCode = hashCode(cacheEntry.getFileId(), cacheEntry.getPageIndex());
    int index = index(hashCode);

    LRUEntry lruEntry = entries[index];

    LRUEntry prevEntry = null;
    while (lruEntry != null
        && (lruEntry.hashCode != hashCode || lruEntry.cacheEntry.getFileId() != fileId || lruEntry.cacheEntry.getPageIndex() != pageIndex)) {
      prevEntry = lruEntry;
      lruEntry = lruEntry.next;
    }

    assert tail == null || tail.before != tail;
    assert tail == null || tail.after == null;

    if (lruEntry == null) {
      lruEntry = new LRUEntry();

      lruEntry.hashCode = hashCode;

      if (prevEntry == null)
        entries[index] = lruEntry;
      else
        prevEntry.next = lruEntry;

      size++;
    }

    lruEntry.cacheEntry = cacheEntry;

    removeFromLRUList(lruEntry);

    if (head == null) {
      head = lruEntry;
      tail = lruEntry;

      lruEntry.before = null;
      lruEntry.after = null;
    } else {
      tail.after = lruEntry;

      lruEntry.before = tail;
      lruEntry.after = null;

      tail = lruEntry;
    }
    assert tail.before != tail;
    assert tail.after == null;

    if (size >= nextThreshold)
      rehash();
  }

  @Override
  public void clear() {
    entries = new LRUEntry[1024];
    nextThreshold = (int) (entries.length * 0.75);

    head = tail = null;
    size = 0;
  }

  private void rehash() {
    long len = entries.length << 1;
    if (len >= Integer.MAX_VALUE) {
      if (entries.length < Integer.MAX_VALUE)
        len = Integer.MAX_VALUE;
      else
        return;
    }

    LRUEntry[] oldLruEntries = entries;

    entries = new LRUEntry[(int) len];
    for (LRUEntry oldLruEntry : oldLruEntries) {
      LRUEntry currentLRUEntry = oldLruEntry;

      while (currentLRUEntry != null) {
        int index = index(currentLRUEntry.hashCode);
        LRUEntry nexEntry = currentLRUEntry.next;
        appendEntry(index, currentLRUEntry);

        currentLRUEntry = nexEntry;
      }
    }

    nextThreshold = (int) (entries.length * 0.75);
  }

  private void appendEntry(int index, LRUEntry entry) {
    LRUEntry lruEntry = entries[index];
    if (lruEntry == null)
      entries[index] = entry;
    else {
      while (lruEntry.next != null)
        lruEntry = lruEntry.next;

      lruEntry.next = entry;
    }

    entry.next = null;
  }

  @Override
  public boolean contains(long fileId, long filePosition) {
    return get(fileId, filePosition) != null;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public OCacheEntry removeLRU() {
    LRUEntry entryToRemove = head;
    while (entryToRemove != null
        && (entryToRemove.cacheEntry.getCachePointer() != null && entryToRemove.cacheEntry.getUsagesCount() != 0)) {
      entryToRemove = entryToRemove.after;
    }
    if (entryToRemove != null) {
      return remove(entryToRemove.cacheEntry.getFileId(), entryToRemove.cacheEntry.getPageIndex());
    } else {
      return null;
    }
  }

  @Override
  public OCacheEntry getLRU() {
    LRUEntry lruEntry = head;
    while (lruEntry != null && (lruEntry.cacheEntry.getCachePointer() != null && lruEntry.cacheEntry.getUsagesCount() != 0)) {
      lruEntry = lruEntry.after;
    }

    if (lruEntry == null)
      return null;

    return lruEntry.cacheEntry;
  }

  @Override
  public Iterator<OCacheEntry> iterator() {
    return new MRUEntryIterator();
  }

  private int index(long hashCode) {
    return (int) ((entries.length - 1) & hashCode);
  }

  private long hashCode(final long fileId, final long filePosition) {
    final byte[] result = new byte[2 * OLongSerializer.LONG_SIZE];
    OLongSerializer.INSTANCE.serializeLiteral(fileId, result, OLongSerializer.LONG_SIZE);
    OLongSerializer.INSTANCE.serializeLiteral(filePosition, result, OLongSerializer.LONG_SIZE);

    return OMurmurHash3.murmurHash3_x64_64(result, SEED);
  }

  private final class MRUEntryIterator implements Iterator<OCacheEntry> {
    private LRUEntry current = tail;

    @Override
    public boolean hasNext() {
      return current != null;
    }

    @Override
    public OCacheEntry next() {
      if (!hasNext())
        throw new NoSuchElementException();

      LRUEntry entry = current;
      current = entry.before;

      return entry.cacheEntry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
