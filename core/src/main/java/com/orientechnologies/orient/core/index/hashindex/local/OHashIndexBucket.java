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
import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * @author Andrey Lomakin
 * @since 2/17/13
 */
public class OHashIndexBucket<K, V> implements Iterable<OHashIndexBucket.Entry<K, V>> {
  private static final int            MAGIC_NUMBER_OFFSET        = 0;
  private static final int            CRC32_OFFSET               = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int            WAL_SEGMENT_OFFSET         = CRC32_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int            WAL_POSITION_OFFSET        = WAL_SEGMENT_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int            FREE_POINTER_OFFSET        = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int            DEPTH_OFFSET               = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int            SIZE_OFFSET                = DEPTH_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int            HISTORY_OFFSET             = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int            NEXT_REMOVED_BUCKET_OFFSET = HISTORY_OFFSET + OLongSerializer.LONG_SIZE * 64;
  private static final int            POSITIONS_ARRAY_OFFSET     = NEXT_REMOVED_BUCKET_OFFSET + OLongSerializer.LONG_SIZE;

  public static final int             MAX_BUCKET_SIZE_BYTES      = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  private final ODirectMemoryPointer  bufferPointer;

  private final OBinarySerializer<K>  keySerializer;
  private final OBinarySerializer<V>  valueSerializer;
  private final OType[]               keyTypes;
  private final OHashFunction<K>      keyHashFunction;
  private final KeyHashCodeComparator comparator;

  public OHashIndexBucket(int depth, ODirectMemoryPointer bufferPointer, OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer, OType[] keyTypes, OHashFunction<K> keyHashFunction) {
    this.bufferPointer = bufferPointer;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.keyTypes = keyTypes;
    this.keyHashFunction = keyHashFunction;

    this.comparator = new KeyHashCodeComparator<K>(keyHashFunction);

    bufferPointer.setByte(DEPTH_OFFSET, (byte) depth);
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(MAX_BUCKET_SIZE_BYTES, bufferPointer, FREE_POINTER_OFFSET);
  }

  public OHashIndexBucket(ODirectMemoryPointer bufferPointer, OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer, OType[] keyTypes, OHashFunction<K> keyHashFunction) {
    this.bufferPointer = bufferPointer;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.keyTypes = keyTypes;
    this.keyHashFunction = keyHashFunction;

    this.comparator = new KeyHashCodeComparator<K>(keyHashFunction);
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
    int entryPosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(bufferPointer, POSITIONS_ARRAY_OFFSET + index
        * OIntegerSerializer.INT_SIZE);

    final K key = keySerializer.deserializeFromDirectMemory(bufferPointer, entryPosition);
    entryPosition += keySerializer.getObjectSizeInDirectMemory(bufferPointer, entryPosition);

    final V value = valueSerializer.deserializeFromDirectMemory(bufferPointer, entryPosition);
    return new Entry<K, V>(key, value);
  }

  public K getKey(int index) {
    int entryPosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(bufferPointer, POSITIONS_ARRAY_OFFSET + index
        * OIntegerSerializer.INT_SIZE);

    return keySerializer.deserializeFromDirectMemory(bufferPointer, entryPosition);
  }

  public int getIndex(final K key) {
    return binarySearch(key);
  }

  public int size() {
    return OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(bufferPointer, SIZE_OFFSET);
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
        + (MAX_BUCKET_SIZE_BYTES - OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(bufferPointer, +FREE_POINTER_OFFSET))
        + buddyBucket.size()
        * OIntegerSerializer.INT_SIZE
        + (MAX_BUCKET_SIZE_BYTES - OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(buddyBucket.bufferPointer,
            FREE_POINTER_OFFSET));
  }

  public int getContentSize() {
    return POSITIONS_ARRAY_OFFSET + size() * OIntegerSerializer.INT_SIZE
        + (MAX_BUCKET_SIZE_BYTES - OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(bufferPointer, FREE_POINTER_OFFSET));
  }

  public Entry<K, V> deleteEntry(int index) {
    final Entry<K, V> removedEntry = getEntry(index);

    final int freePointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(bufferPointer, FREE_POINTER_OFFSET);

    final int positionOffset = POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE;
    final int entryPosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(bufferPointer, positionOffset);

    final int keySize = keySerializer.getObjectSizeInDirectMemory(bufferPointer, entryPosition);
    final int ridSize = valueSerializer.getObjectSizeInDirectMemory(bufferPointer, entryPosition + keySize);
    final int entrySize = keySize + ridSize;

    bufferPointer.copyData(positionOffset + OIntegerSerializer.INT_SIZE, bufferPointer, positionOffset, size()
        * OIntegerSerializer.INT_SIZE - (index + 1) * OIntegerSerializer.INT_SIZE);

    if (entryPosition > freePointer)
      bufferPointer.copyData(freePointer, bufferPointer, freePointer + entrySize, entryPosition - freePointer);

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;
    int size = size();
    for (int i = 0; i < size - 1; i++) {
      int currentEntryPosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(bufferPointer, currentPositionOffset);
      if (currentEntryPosition < entryPosition)
        OIntegerSerializer.INSTANCE.serializeInDirectMemory(currentEntryPosition + entrySize, bufferPointer, currentPositionOffset);
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(freePointer + entrySize, bufferPointer, FREE_POINTER_OFFSET);

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(size - 1, bufferPointer, SIZE_OFFSET);

    return removedEntry;
  }

  public boolean addEntry(K key, V value) {
    int entreeSize = keySerializer.getObjectSize(key, keyTypes) + valueSerializer.getObjectSize(value);
    int freePointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(bufferPointer, FREE_POINTER_OFFSET);

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
    int entreeSize = keySerializer.getObjectSize(key, keyTypes) + valueSerializer.getObjectSize(value);
    int freePointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(bufferPointer, FREE_POINTER_OFFSET);
    int size = size();
    final int positionsOffset = insertionPoint * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;

    bufferPointer.copyData(positionsOffset, bufferPointer, positionsOffset + OIntegerSerializer.INT_SIZE, size()
        * OIntegerSerializer.INT_SIZE - insertionPoint * OIntegerSerializer.INT_SIZE);

    final int entreePosition = freePointer - entreeSize;
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(entreePosition, bufferPointer, positionsOffset);
    serializeEntry(key, value, entreePosition);

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(entreePosition, bufferPointer, FREE_POINTER_OFFSET);

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(size + 1, bufferPointer, SIZE_OFFSET);
  }

  public void appendEntry(K key, V value) {
    final int positionsOffset = size() * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;
    final int entreeSize = keySerializer.getObjectSize(key, keyTypes) + valueSerializer.getObjectSize(value);

    final int freePointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(bufferPointer, FREE_POINTER_OFFSET);
    final int entreePosition = freePointer - entreeSize;

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(entreePosition, bufferPointer, positionsOffset);
    serializeEntry(key, value, entreePosition);

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(freePointer - entreeSize, bufferPointer, FREE_POINTER_OFFSET);

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(size() + 1, bufferPointer, SIZE_OFFSET);
  }

  private void serializeEntry(K key, V value, int entryOffset) {
    keySerializer.serializeInDirectMemory(key, bufferPointer, entryOffset, keyTypes);
    entryOffset += keySerializer.getObjectSize(key, keyTypes);

    valueSerializer.serializeInDirectMemory(value, bufferPointer, entryOffset);
  }

  public int getDepth() {
    return bufferPointer.getByte(DEPTH_OFFSET);
  }

  public void setDepth(int depth) {
    bufferPointer.setByte(DEPTH_OFFSET, (byte) depth);
  }

  public long getNextRemovedBucketPair() {
    return OLongSerializer.INSTANCE.deserializeFromDirectMemory(bufferPointer, NEXT_REMOVED_BUCKET_OFFSET);
  }

  public void setNextRemovedBucketPair(long nextRemovedBucketPair) {
    OLongSerializer.INSTANCE.serializeInDirectMemory(nextRemovedBucketPair, bufferPointer, NEXT_REMOVED_BUCKET_OFFSET);
  }

  public long getSplitHistory(int level) {
    return OLongSerializer.INSTANCE.deserializeFromDirectMemory(bufferPointer, HISTORY_OFFSET + OLongSerializer.LONG_SIZE * level);
  }

  public void setSplitHistory(int level, long position) {
    OLongSerializer.INSTANCE.serializeInDirectMemory(position, bufferPointer, HISTORY_OFFSET + OLongSerializer.LONG_SIZE * level);
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

  protected static final class KeyHashCodeComparator<K> implements Comparator<K> {
    private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

    private final OHashFunction<K>      keyHashFunction;

    public KeyHashCodeComparator(OHashFunction<K> keyHashFunction) {
      this.keyHashFunction = keyHashFunction;
    }

    @Override
    public int compare(K keyOne, K keyTwo) {
      final long hashCodeOne = keyHashFunction.hashCode(keyOne);
      final long hashCodeTwo = keyHashFunction.hashCode(keyTwo);

      if (hashCodeOne > hashCodeTwo)
        return 1;
      if (hashCodeOne < hashCodeTwo)
        return -1;

      return comparator.compare(keyOne, keyTwo);
    }
  }
}
