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
package com.orientechnologies.orient.core.index.hashindex.local.arc;

import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.orientechnologies.common.hash.OMurmurHash3;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 25.02.13
 */
public class LRUList implements Iterable<LRUEntry> {
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

  public LRUEntry get(String fileName, long filePosition) {
    long hashCode = hashCode(fileName, filePosition);
    int index = index(hashCode);

    LRUEntry lruEntry = entries[index];

    while (lruEntry != null
        && (lruEntry.hashCode != hashCode || lruEntry.filePosition != filePosition || !lruEntry.fileName.equals(fileName)))
      lruEntry = lruEntry.next;

    return lruEntry;
  }

  public LRUEntry remove(String fileName, long filePosition) {
    long hashCode = hashCode(fileName, filePosition);
    int index = index(hashCode);

    LRUEntry lruEntry = entries[index];

    LRUEntry prevEntry = null;
    while (lruEntry != null
        && (lruEntry.hashCode != hashCode || !lruEntry.fileName.equals(fileName) || lruEntry.filePosition != filePosition)) {
      prevEntry = lruEntry;
      lruEntry = lruEntry.next;
    }

    if (lruEntry == null)
      return null;

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

    if (prevEntry == null)
      entries[index] = lruEntry.next;
    else
      prevEntry.next = lruEntry.next;

    size--;

    return lruEntry;
  }

  public LRUEntry putToMRU(String fileName, long filePosition, long dataPointer, boolean isDirty) {
    long hashCode = hashCode(fileName, filePosition);
    int index = index(hashCode);

    LRUEntry lruEntry = entries[index];

    LRUEntry prevEntry = null;
    while (lruEntry != null
        && (lruEntry.hashCode != hashCode || !lruEntry.fileName.equals(fileName) || lruEntry.filePosition != filePosition)) {
      prevEntry = lruEntry;
      lruEntry = lruEntry.next;
    }

    if (lruEntry == null) {
      lruEntry = new LRUEntry();

      lruEntry.filePosition = filePosition;
      lruEntry.fileName = fileName;
      lruEntry.hashCode = hashCode;

      if (prevEntry == null)
        entries[index] = lruEntry;
      else
        prevEntry.next = lruEntry;

      size++;
    }

    lruEntry.dataPointer = dataPointer;
    lruEntry.isDirty = isDirty;

    LRUEntry before = lruEntry.before;
    LRUEntry after = lruEntry.after;

    if (before != null)
      before.after = after;
    if (after != null)
      after.before = before;

    if (head == null) {
      head = lruEntry;
      tail = lruEntry;
    } else {
      tail.after = lruEntry;
      lruEntry.before = tail;
      tail = lruEntry;
    }

    if (size >= nextThreshold)
      rehash();

    return lruEntry;
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

  public boolean contains(String fileName, long filePosition) {
    return get(fileName, filePosition) != null;
  }

  public int size() {
    return size;
  }

  public LRUEntry removeLRU() {
    return remove(head.fileName, head.filePosition);
  }

  @Override
  public Iterator<LRUEntry> iterator() {
    return new MRUEntryIterator();
  }

  private int index(long hashCode) {
    return (int) ((entries.length - 1) & hashCode);
  }

  private long hashCode(String fileName, long filePosition) {
    final byte[] stringBytes;
    try {
      stringBytes = fileName.getBytes("UTF-8");
      final byte[] result = new byte[stringBytes.length + OLongSerializer.LONG_SIZE];
      System.arraycopy(stringBytes, 0, result, 0, stringBytes.length);
      OLongSerializer.INSTANCE.serialize(filePosition, result, stringBytes.length);

      return OMurmurHash3.murmurHash3_x64_64(result, SEED);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("Exception during hash code calculation", e);
    }
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
