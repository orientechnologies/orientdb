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
package com.orientechnologies.orient.core.index.hashindex.local;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

/**
 * @author Andrey Lomakin
 * @since 2/17/13
 */
public class OHashIndexBucket<K, V> implements Iterable<OHashIndexBucket.Entry<K, V>> {
  private static final int            FREE_POINTER_OFFSET        = 0;
  private static final int            DEPTH_OFFSET               = OIntegerSerializer.INT_SIZE;
  private static final int            SIZE_OFFSET                = DEPTH_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int            HISTORY_OFFSET             = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int            NEXT_REMOVED_BUCKET_OFFSET = HISTORY_OFFSET + OLongSerializer.LONG_SIZE * 64;
  private static final int            POSITIONS_ARRAY_OFFSET     = NEXT_REMOVED_BUCKET_OFFSET + OLongSerializer.LONG_SIZE;

  public static final int             MAX_BUCKET_SIZE_BYTES      = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger();

  private final long                  bufferPointer;
  private final ODirectMemory         directMemory;

  private final Comparator<? super K> comparator                 = ODefaultComparator.INSTANCE;

  private final OBinarySerializer<K>  keySerializer;
  private final OBinarySerializer<V>  valueSerializer;

  public OHashIndexBucket(int depth, long bufferPointer, ODirectMemory directMemory, OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer) {
    this.bufferPointer = bufferPointer;
    this.directMemory = directMemory;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;

    directMemory.setByte(bufferPointer + DEPTH_OFFSET, (byte) depth);
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(MAX_BUCKET_SIZE_BYTES, directMemory, bufferPointer + FREE_POINTER_OFFSET);
  }

  public OHashIndexBucket(long bufferPointer, ODirectMemory directMemory, OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer) {
    this.bufferPointer = bufferPointer;
    this.directMemory = directMemory;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public Entry<K, V> find(final K key) {
    final int index = binarySearch(key);
    if (index < 0)
      return null;

    return getEntry(index);
  }

  private int binarySearch(K key) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      K midVal = getKey(mid);
      int cmp = comparator.compare(midVal, key);

      if (cmp < 0)
        low = mid + 1;
      else if (cmp > 0)
        high = mid - 1;
      else
        return mid; // key found
    }
    return -(low + 1); // key not found.
  }

  public Entry<K, V> getEntry(int index) {
    int entryPosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, bufferPointer
        + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    final K key = keySerializer.deserializeFromDirectMemory(directMemory, bufferPointer + entryPosition);
    entryPosition += keySerializer.getObjectSizeInDirectMemory(directMemory, bufferPointer + entryPosition);

    final V value = valueSerializer.deserializeFromDirectMemory(directMemory, bufferPointer + entryPosition);
    return new Entry<K, V>(key, value);
  }

  public K getKey(int index) {
    int entryPosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, bufferPointer
        + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    return keySerializer.deserializeFromDirectMemory(directMemory, bufferPointer + entryPosition);
  }

  public int getIndex(final K key) {
    return binarySearch(key);
  }

  public int size() {
    return OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, bufferPointer + SIZE_OFFSET);
  }

  public Iterator<Entry<K, V>> iterator() {
    return new EntryIterator(0);
  }

  public Iterator<Entry<K, V>> iterator(int index) {
    return new EntryIterator(index);
  }

  public int mergedSize(OHashIndexBucket buddyBucket) {
    return POSITIONS_ARRAY_OFFSET
        + size()
        * OIntegerSerializer.INT_SIZE
        + (MAX_BUCKET_SIZE_BYTES - OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, bufferPointer
            + FREE_POINTER_OFFSET))
        + buddyBucket.size()
        * OIntegerSerializer.INT_SIZE
        + (MAX_BUCKET_SIZE_BYTES - OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, buddyBucket.bufferPointer
            + FREE_POINTER_OFFSET));
  }

  public int getContentSize() {
    return POSITIONS_ARRAY_OFFSET
        + size()
        * OIntegerSerializer.INT_SIZE
        + (MAX_BUCKET_SIZE_BYTES - OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, bufferPointer
            + FREE_POINTER_OFFSET));
  }

  public Entry<K, V> deleteEntry(int index) {
    final Entry<K, V> removedEntry = getEntry(index);

    final int freePointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, bufferPointer
        + FREE_POINTER_OFFSET);

    final int positionOffset = POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE;
    final int entryPosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, bufferPointer + positionOffset);

    final int keySize = keySerializer.getObjectSizeInDirectMemory(directMemory, bufferPointer + entryPosition);
    final int ridSize = valueSerializer.getObjectSizeInDirectMemory(directMemory, bufferPointer + entryPosition + keySize);
    final int entrySize = keySize + ridSize;

    directMemory.copyData(bufferPointer + positionOffset + OIntegerSerializer.INT_SIZE, bufferPointer + positionOffset, size()
        * OIntegerSerializer.INT_SIZE - (index + 1) * OIntegerSerializer.INT_SIZE);

    if (entryPosition > freePointer)
      directMemory.copyData(bufferPointer + freePointer, bufferPointer + freePointer + entrySize, entryPosition - freePointer);

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;
    int size = size();
    for (int i = 0; i < size - 1; i++) {
      int currentEntryPosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, bufferPointer
          + currentPositionOffset);
      if (currentEntryPosition < entryPosition)
        OIntegerSerializer.INSTANCE.serializeInDirectMemory(currentEntryPosition + entrySize, directMemory, bufferPointer
            + currentPositionOffset);
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(freePointer + entrySize, directMemory, bufferPointer + FREE_POINTER_OFFSET);

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(size - 1, directMemory, bufferPointer + SIZE_OFFSET);

    return removedEntry;
  }

  public boolean addEntry(K key, V value) {
    int entreeSize = keySerializer.getObjectSize(key) + valueSerializer.getObjectSize(value);
    int freePointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, bufferPointer + FREE_POINTER_OFFSET);

    int size = size();
    if (freePointer - entreeSize < POSITIONS_ARRAY_OFFSET + (size + 1) * OIntegerSerializer.INT_SIZE)
      return false;

    final int index = binarySearch(key);
    if (index >= 0)
      throw new IllegalArgumentException("Given value is present in bucket.");

    final int insertionPoint = -index - 1;
    insertEntry(key, value, insertionPoint);

    return true;
  }

  private void insertEntry(K key, V value, int insertionPoint) {
    int entreeSize = keySerializer.getObjectSize(key) + valueSerializer.getObjectSize(value);
    int freePointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, bufferPointer + FREE_POINTER_OFFSET);
    int size = size();
    final int positionsOffset = insertionPoint * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;

    directMemory.copyData(bufferPointer + positionsOffset, bufferPointer + positionsOffset + OIntegerSerializer.INT_SIZE, size()
        * OIntegerSerializer.INT_SIZE - insertionPoint * OIntegerSerializer.INT_SIZE);

    final int entreePosition = freePointer - entreeSize;
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(entreePosition, directMemory, bufferPointer + positionsOffset);
    serializeEntry(key, value, entreePosition);

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(entreePosition, directMemory, bufferPointer + FREE_POINTER_OFFSET);

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(size + 1, directMemory, bufferPointer + SIZE_OFFSET);
  }

  public void appendEntry(K key, V value) {
    final int positionsOffset = size() * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;
    final int entreeSize = keySerializer.getObjectSize(key) + valueSerializer.getObjectSize(value);

    final int freePointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, bufferPointer
        + FREE_POINTER_OFFSET);
    final int entreePosition = freePointer - entreeSize;

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(entreePosition, directMemory, bufferPointer + positionsOffset);
    serializeEntry(key, value, entreePosition);

    OIntegerSerializer.INSTANCE
        .serializeInDirectMemory(freePointer - entreeSize, directMemory, bufferPointer + FREE_POINTER_OFFSET);

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(size() + 1, directMemory, bufferPointer + SIZE_OFFSET);
  }

  private void serializeEntry(K key, V value, int entryOffset) {
    keySerializer.serializeInDirectMemory(key, directMemory, bufferPointer + entryOffset);
    entryOffset += keySerializer.getObjectSize(key);

    valueSerializer.serializeInDirectMemory(value, directMemory, bufferPointer + entryOffset);
  }

  public int getDepth() {
    return directMemory.getByte(bufferPointer + DEPTH_OFFSET);
  }

  public void setDepth(int depth) {
    directMemory.setByte(bufferPointer + DEPTH_OFFSET, (byte) depth);
  }

  public long getNextRemovedBucketPair() {
    return OLongSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, bufferPointer + NEXT_REMOVED_BUCKET_OFFSET);
  }

  public void setNextRemovedBucketPair(long nextRemovedBucketPair) {
    OLongSerializer.INSTANCE.serializeInDirectMemory(nextRemovedBucketPair, directMemory, bufferPointer
        + NEXT_REMOVED_BUCKET_OFFSET);
  }

  public void updateEntry(int index, V value) {
    int entryPosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, bufferPointer
        + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);
    entryPosition += keySerializer.getObjectSizeInDirectMemory(directMemory, bufferPointer + entryPosition);

    if (valueSerializer.getObjectSize(value) == valueSerializer.getObjectSizeInDirectMemory(directMemory, bufferPointer
        + entryPosition))
      valueSerializer.serializeInDirectMemory(value, directMemory, bufferPointer + entryPosition);
    else {
      K key = getKey(index);

      deleteEntry(index);
      insertEntry(key, value, index);
    }
  }

  public long getSplitHistory(int level) {
    return OLongSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, bufferPointer + HISTORY_OFFSET
        + OLongSerializer.LONG_SIZE * level);
  }

  public void setSplitHistory(int level, long position) {
    OLongSerializer.INSTANCE.serializeInDirectMemory(position, directMemory, bufferPointer + HISTORY_OFFSET
        + OLongSerializer.LONG_SIZE * level);
  }

  public static class Entry<K, V> {
    public final K key;
    public final V value;

    public Entry(K key, V value) {
      this.key = key;
      this.value = value;
    }
  }

  private final class EntryIterator implements Iterator<Entry<K, V>> {
    private int currentIndex;

    private EntryIterator(int currentIndex) {
      this.currentIndex = currentIndex;
    }

    @Override
    public boolean hasNext() {
      return currentIndex < size();
    }

    @Override
    public Entry<K, V> next() {
      if (currentIndex >= size())
        throw new NoSuchElementException("Iterator was reached last element");

      final Entry<K, V> entry = getEntry(currentIndex);
      currentIndex++;
      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove operation is not supported");
    }
  }
}
