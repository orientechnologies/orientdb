/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.orientechnologies.common.hash.OMurmurHash3;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 25.02.13
 */
class LRUList implements Iterable<LRUEntry> {
  private static final int SEED = 362498820;

  private LRUEntry         head;
  private LRUEntry         tail;

  private int              nextThreshold;
  private int              size;

  private LRUEntry         entries[];

  public LRUList() {
    entries = new LRUEntry[1024];
    nextThreshold = (int) (entries.length * 0.75);
  }

  public LRUEntry get(long fileId, long pageIndex) {
    long hashCode = hashCode(fileId, pageIndex);
    int index = index(hashCode);

    LRUEntry lruEntry = entries[index];

    while (lruEntry != null && (lruEntry.hashCode != hashCode || lruEntry.pageIndex != pageIndex || lruEntry.fileId != fileId))
      lruEntry = lruEntry.next;

    return lruEntry;
  }

  public LRUEntry remove(long fileId, long pageIndex) {
    long hashCode = hashCode(fileId, pageIndex);
    int index = index(hashCode);

    LRUEntry lruEntry = entries[index];

    LRUEntry prevEntry = null;
    while (lruEntry != null && (lruEntry.hashCode != hashCode || lruEntry.fileId != fileId || lruEntry.pageIndex != pageIndex)) {
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

    return lruEntry;
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

  public LRUEntry putToMRU(long fileId, long pageIndex, long dataPointer, boolean isDirty, boolean managedExternally) {
    long hashCode = hashCode(fileId, pageIndex);
    int index = index(hashCode);

    LRUEntry lruEntry = entries[index];

    LRUEntry prevEntry = null;
    while (lruEntry != null && (lruEntry.hashCode != hashCode || lruEntry.fileId != fileId || lruEntry.pageIndex != pageIndex)) {
      prevEntry = lruEntry;
      lruEntry = lruEntry.next;
    }

    assert tail == null || tail.before != tail;
    assert tail == null || tail.after == null;

    if (lruEntry == null) {
      lruEntry = new LRUEntry();

      lruEntry.pageIndex = pageIndex;
      lruEntry.fileId = fileId;
      lruEntry.hashCode = hashCode;

      if (prevEntry == null)
        entries[index] = lruEntry;
      else
        prevEntry.next = lruEntry;

      size++;
    }

    lruEntry.dataPointer = dataPointer;
    lruEntry.isDirty = isDirty;
    lruEntry.managedExternally = managedExternally;

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

    return lruEntry;
  }

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

  public boolean contains(long fileId, long filePosition) {
    return get(fileId, filePosition) != null;
  }

  public int size() {
    return size;
  }

  public LRUEntry removeLRU() {
    LRUEntry entryToRemove = head;
    while (entryToRemove.usageCounter.get() != 0) {
      entryToRemove = entryToRemove.next;
    }
    return remove(entryToRemove.fileId, entryToRemove.pageIndex);
  }

  public LRUEntry getLRU() {
    return head;
  }

  @Override
  public Iterator<LRUEntry> iterator() {
    return new MRUEntryIterator();
  }

  private int index(long hashCode) {
    return (int) ((entries.length - 1) & hashCode);
  }

  private long hashCode(long fileId, long filePosition) {
    final byte[] result = new byte[2 * OLongSerializer.LONG_SIZE];
    OLongSerializer.INSTANCE.serialize(fileId, result, OLongSerializer.LONG_SIZE);
    OLongSerializer.INSTANCE.serialize(filePosition, result, OLongSerializer.LONG_SIZE);

    return OMurmurHash3.murmurHash3_x64_64(result, SEED);
  }

  private final class MRUEntryIterator implements Iterator<LRUEntry> {
    private LRUEntry current = tail;

    @Override
    public boolean hasNext() {
      return current != null;
    }

    @Override
    public LRUEntry next() {
      if (!hasNext())
        throw new NoSuchElementException();

      LRUEntry entry = current;
      current = entry.before;

      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
