/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.storage.index.hashindex.local;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 2/17/13
 */
public final class OHashIndexBucket<K, V> extends ODurablePage implements Iterable<OHashIndexBucket.Entry<K, V>> {
  private static final int FREE_POINTER_OFFSET = NEXT_FREE_POSITION;
  private static final int DEPTH_OFFSET        = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int SIZE_OFFSET         = DEPTH_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int HISTORY_OFFSET      = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int NEXT_REMOVED_BUCKET_OFFSET = HISTORY_OFFSET + OLongSerializer.LONG_SIZE * 64;
  private static final int POSITIONS_ARRAY_OFFSET     = NEXT_REMOVED_BUCKET_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int MAX_BUCKET_SIZE_BYTES = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  private final OBinarySerializer<K> keySerializer;
  private final OBinarySerializer<V> valueSerializer;
  private final Comparator           keyComparator = ODefaultComparator.INSTANCE;

  private final OEncryption encryption;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  OHashIndexBucket(int depth, OCacheEntry cacheEntry, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer,
      OEncryption encryption) {
    super(cacheEntry);

    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.encryption = encryption;

    init(depth);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  OHashIndexBucket(OCacheEntry cacheEntry, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer,
      OEncryption encryption) {
    super(cacheEntry);

    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.encryption = encryption;
  }

  public void init(int depth) {
    setByteValue(DEPTH_OFFSET, (byte) depth);
    setIntValue(FREE_POINTER_OFFSET, MAX_BUCKET_SIZE_BYTES);
    setIntValue(SIZE_OFFSET, 0);
  }

  public Entry<K, V> find(final K key, final long hashCode) {
    final int index = binarySearch(key, hashCode);
    if (index < 0) {
      return null;
    }

    return getEntry(index);
  }

  private int binarySearch(K key, long hashCode) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;

      final long midHashCode = getHashCode(mid);
      final int cmp;
      if (lessThanUnsigned(midHashCode, hashCode))
        cmp = -1;
      else if (greaterThanUnsigned(midHashCode, hashCode))
        cmp = 1;
      else {
        final K midVal = getKey(mid);
        //noinspection unchecked
        cmp = keyComparator.compare(midVal, key);
      }

      if (cmp < 0)
        low = mid + 1;
      else if (cmp > 0)
        high = mid - 1;
      else
        return mid; // key found
    }
    return -(low + 1); // key not found.
  }

  private static boolean lessThanUnsigned(long longOne, long longTwo) {
    return (longOne + Long.MIN_VALUE) < (longTwo + Long.MIN_VALUE);
  }

  private static boolean greaterThanUnsigned(long longOne, long longTwo) {
    return (longOne + Long.MIN_VALUE) > (longTwo + Long.MIN_VALUE);
  }

  public Entry<K, V> getEntry(int index) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    final long hashCode = getLongValue(entryPosition);
    entryPosition += OLongSerializer.LONG_SIZE;

    final K key;

    if (encryption == null) {
      key = deserializeFromDirectMemory(keySerializer, entryPosition);
      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedSize = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final byte[] encryptedKey = getBinaryValue(entryPosition, encryptedSize);
      entryPosition += encryptedSize;

      final byte[] serializedKey = encryption.decrypt(encryptedKey);
      key = keySerializer.deserializeNativeObject(serializedKey, 0);
    }

    final V value = deserializeFromDirectMemory(valueSerializer, entryPosition);
    return new Entry<>(key, value, hashCode);
  }

  RawEntry getRawEntry(int index) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    final long hashCode = getLongValue(entryPosition);
    entryPosition += OLongSerializer.LONG_SIZE;

    final byte[] rawKey;
    if (encryption == null) {
      final int keyLen = getObjectSizeInDirectMemory(keySerializer, entryPosition);
      rawKey = getBinaryValue(entryPosition, keyLen);

      entryPosition += rawKey.length;
    } else {
      final int encryptedLen = getIntValue(entryPosition);
      rawKey = getBinaryValue(entryPosition, encryptedLen + OIntegerSerializer.INT_SIZE);
      entryPosition += encryptedLen + OIntegerSerializer.INT_SIZE;
    }

    final int valueLen = getObjectSizeInDirectMemory(valueSerializer, entryPosition);
    final byte[] rawValue = getBinaryValue(entryPosition, valueLen);

    return new RawEntry(hashCode, rawKey, rawValue);
  }

  /**
   * Obtains the value stored under the given index in this bucket.
   *
   * @param index the value index.
   *
   * @return the obtained value.
   */
  public V getValue(int index) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    // skip hash code
    entryPosition += OLongSerializer.LONG_SIZE;

    // skip key
    if (encryption == null) {
      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedSize = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE + encryptedSize;
    }

    return deserializeFromDirectMemory(valueSerializer, entryPosition);
  }

  byte[] getRawValue(int index) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    // skip hash code
    entryPosition += OLongSerializer.LONG_SIZE;

    // skip key
    if (encryption == null) {
      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedSize = getIntValue(entryPosition);
      entryPosition += encryptedSize + OIntegerSerializer.INT_SIZE;
    }

    final int valSize = getObjectSizeInDirectMemory(valueSerializer, entryPosition);
    return getBinaryValue(entryPosition, valSize);
  }

  private long getHashCode(int index) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);
    return getLongValue(entryPosition);
  }

  public K getKey(int index) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    if (encryption == null) {
      return deserializeFromDirectMemory(keySerializer, entryPosition + OLongSerializer.LONG_SIZE);
    }

    entryPosition += OLongSerializer.LONG_SIZE;
    final int encryptedSize = getIntValue(entryPosition);
    entryPosition += OIntegerSerializer.INT_SIZE;

    final byte[] encryptedKey = getBinaryValue(entryPosition, encryptedSize);
    return keySerializer.deserializeNativeObject(encryption.decrypt(encryptedKey), 0);
  }

  public int getIndex(final long hashCode, final K key) {
    return binarySearch(key, hashCode);
  }

  public int size() {
    return getIntValue(SIZE_OFFSET);
  }

  public Iterator<Entry<K, V>> iterator() {
    return new EntryIterator(0);
  }

  public Iterator<Entry<K, V>> iterator(int index) {
    return new EntryIterator(index);
  }

  public int getContentSize() {
    return POSITIONS_ARRAY_OFFSET + size() * OIntegerSerializer.INT_SIZE + (MAX_BUCKET_SIZE_BYTES - getIntValue(
        FREE_POINTER_OFFSET));
  }

  void updateEntry(int index, byte[] value) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);
    entryPosition += OLongSerializer.LONG_SIZE;

    if (encryption == null) {
      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      entryPosition += getIntValue(entryPosition) + OIntegerSerializer.INT_SIZE;
    }

    setBinaryValue(entryPosition, value);
  }

  void deleteEntry(int index) {
    final int freePointer = getIntValue(FREE_POINTER_OFFSET);

    final int positionOffset = POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE;
    final int entryPosition = getIntValue(positionOffset);

    final int keySize;
    if (encryption == null) {
      keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition + OLongSerializer.LONG_SIZE);
    } else {
      keySize = getIntValue(entryPosition + OLongSerializer.LONG_SIZE) + OIntegerSerializer.INT_SIZE;
    }

    final int ridSize = getObjectSizeInDirectMemory(valueSerializer, entryPosition + keySize + OLongSerializer.LONG_SIZE);
    final int entrySize = keySize + ridSize + OLongSerializer.LONG_SIZE;

    moveData(positionOffset + OIntegerSerializer.INT_SIZE, positionOffset,
        size() * OIntegerSerializer.INT_SIZE - (index + 1) * OIntegerSerializer.INT_SIZE);

    if (entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;
    int size = size();
    for (int i = 0; i < size - 1; i++) {
      int currentEntryPosition = getIntValue(currentPositionOffset);
      if (currentEntryPosition < entryPosition)
        setIntValue(currentPositionOffset, currentEntryPosition + entrySize);
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    setIntValue(FREE_POINTER_OFFSET, freePointer + entrySize);
    setIntValue(SIZE_OFFSET, size - 1);
  }

  int entryInsertionIndex(long hashCode, K key, int keyLen, int valueLen) {
    int entreeSize = keyLen + valueLen + OLongSerializer.LONG_SIZE;
    int freePointer = getIntValue(FREE_POINTER_OFFSET);

    int size = size();
    if (freePointer - entreeSize < POSITIONS_ARRAY_OFFSET + (size + 1) * OIntegerSerializer.INT_SIZE) {
      return -1;
    }

    final int index = binarySearch(key, hashCode);
    if (index >= 0) {
      throw new IllegalArgumentException("Given value is present in bucket.");
    }

    return -index - 1;
  }

  void insertEntry(long hashCode, byte[] key, byte[] value, int insertionPoint) {
    final int entreeSize = key.length + value.length + OLongSerializer.LONG_SIZE;
    insertEntry(hashCode, key, value, insertionPoint, entreeSize);
  }

  private void insertEntry(long hashCode, byte[] key, byte[] value, int insertionPoint, int entreeSize) {
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    int size = size();

    final int positionsOffset = insertionPoint * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;

    moveData(positionsOffset, positionsOffset + OIntegerSerializer.INT_SIZE,
        size() * OIntegerSerializer.INT_SIZE - insertionPoint * OIntegerSerializer.INT_SIZE);

    final int entreePosition = freePointer - entreeSize;
    setIntValue(positionsOffset, entreePosition);
    serializeEntry(hashCode, key, value, entreePosition);

    setIntValue(FREE_POINTER_OFFSET, entreePosition);
    setIntValue(SIZE_OFFSET, size + 1);
  }

  void appendEntry(long hashCode, byte[] key, byte[] value) {
    final int positionsOffset = size() * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;
    final int entreeSize = key.length + value.length + OLongSerializer.LONG_SIZE;

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);
    final int entreePosition = freePointer - entreeSize;

    setIntValue(positionsOffset, entreePosition);
    serializeEntry(hashCode, key, value, entreePosition);

    setIntValue(FREE_POINTER_OFFSET, freePointer - entreeSize);
    setIntValue(SIZE_OFFSET, size() + 1);
  }

  private void serializeEntry(long hashCode, byte[] key, byte[] value, int entryOffset) {
    setLongValue(entryOffset, hashCode);
    entryOffset += OLongSerializer.LONG_SIZE;

    setBinaryValue(entryOffset, key);

    entryOffset += key.length;
    setBinaryValue(entryOffset, value);
  }

  public int getDepth() {
    return getByteValue(DEPTH_OFFSET);
  }

  public void setDepth(int depth) {
    setByteValue(DEPTH_OFFSET, (byte) depth);
  }

  public static final class RawEntry {
    public final long   hashCode;
    final        byte[] rawKey;
    final        byte[] rawValue;

    RawEntry(long hashCode, byte[] rawKey, byte[] rawValue) {
      this.hashCode = hashCode;
      this.rawKey = rawKey;
      this.rawValue = rawValue;
    }
  }

  public static class Entry<K, V> {
    public final K    key;
    public final V    value;
    public final long hashCode;

    public Entry(K key, V value, long hashCode) {
      this.key = key;
      this.value = value;
      this.hashCode = hashCode;
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
