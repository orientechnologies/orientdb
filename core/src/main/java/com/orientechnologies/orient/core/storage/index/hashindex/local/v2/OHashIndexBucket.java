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
package com.orientechnologies.orient.core.storage.index.hashindex.local.v2;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 2/17/13
 */
public class OHashIndexBucket<K, V> extends ODurablePage implements Iterable<OHashIndexBucket.Entry<K, V>> {
  private static final int FREE_POINTER_OFFSET = NEXT_FREE_POSITION;
  private static final int DEPTH_OFFSET        = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int SIZE_OFFSET         = DEPTH_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int HISTORY_OFFSET      = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int NEXT_REMOVED_BUCKET_OFFSET = HISTORY_OFFSET + OLongSerializer.LONG_SIZE * 64;
  private static final int POSITIONS_ARRAY_OFFSET     = NEXT_REMOVED_BUCKET_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int MAX_BUCKET_SIZE_BYTES = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  private final OBinarySerializer<K> keySerializer;
  private final OBinarySerializer<V> valueSerializer;
  private final OType[]              keyTypes;
  private final Comparator           keyComparator = ODefaultComparator.INSTANCE;
  private final OEncryption          encryption;

  OHashIndexBucket(int depth, OCacheEntry cacheEntry, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer,
      OType[] keyTypes, OEncryption encryption) throws IOException {
    super(cacheEntry);

    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.keyTypes = keyTypes;
    this.encryption = encryption;

    init(depth);
  }

  OHashIndexBucket(OCacheEntry cacheEntry, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer,
      OType[] keyTypes, OEncryption encryption) {
    super(cacheEntry);

    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.keyTypes = keyTypes;
    this.encryption = encryption;
  }

  public void init(int depth) throws IOException {
    setByteValue(DEPTH_OFFSET, (byte) depth);
    setIntValue(FREE_POINTER_OFFSET, MAX_BUCKET_SIZE_BYTES);
    setIntValue(SIZE_OFFSET, 0);
  }

  public Entry<K, V> find(final K key, final long hashCode) {
    final int index = binarySearch(key, hashCode);
    if (index < 0)
      return null;

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
      final int encryptedLength = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final byte[] encryptedKey = getBinaryValue(entryPosition, encryptedLength);
      entryPosition += encryptedLength;

      final byte[] binaryKey = encryption.decrypt(encryptedKey);

      key = keySerializer.deserializeNativeObject(binaryKey, 0);
    }

    final V value = deserializeFromDirectMemory(valueSerializer, entryPosition);
    return new Entry<K, V>(key, value, hashCode);
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

    if (encryption == null) {
      // skip key
      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedLength = getIntValue(entryPosition);
      entryPosition += encryptedLength + OIntegerSerializer.INT_SIZE;
    }

    return deserializeFromDirectMemory(valueSerializer, entryPosition);
  }

  private long getHashCode(int index) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);
    return getLongValue(entryPosition);
  }

  public K getKey(int index) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    if (encryption == null) {
      return deserializeFromDirectMemory(keySerializer, entryPosition + OLongSerializer.LONG_SIZE);
    } else {
      final int encryptedLength = getIntValue(entryPosition + OLongSerializer.LONG_SIZE);
      final byte[] encryptedBinaryKey = getBinaryValue(entryPosition + OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE,
          encryptedLength);
      final byte[] decryptedBinaryKey = encryption.decrypt(encryptedBinaryKey);
      return keySerializer.deserializeNativeObject(decryptedBinaryKey, 0);
    }
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

  public int mergedSize(OHashIndexBucket buddyBucket) {
    return POSITIONS_ARRAY_OFFSET + size() * OIntegerSerializer.INT_SIZE + (MAX_BUCKET_SIZE_BYTES - getIntValue(
        FREE_POINTER_OFFSET)) + buddyBucket.size() * OIntegerSerializer.INT_SIZE + (MAX_BUCKET_SIZE_BYTES - getIntValue(
        FREE_POINTER_OFFSET));
  }

  public int getContentSize() {
    return POSITIONS_ARRAY_OFFSET + size() * OIntegerSerializer.INT_SIZE + (MAX_BUCKET_SIZE_BYTES - getIntValue(
        FREE_POINTER_OFFSET));
  }

  int updateEntry(int index, V value) throws IOException {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);
    entryPosition += OLongSerializer.LONG_SIZE;

    if (encryption == null) {
      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedSize = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE + encryptedSize;
    }

    final int newSize = valueSerializer.getObjectSize(value);
    final int oldSize = getObjectSizeInDirectMemory(valueSerializer, entryPosition);
    if (newSize != oldSize)
      return -1;

    byte[] newSerializedValue = new byte[newSize];
    valueSerializer.serializeNativeObject(value, newSerializedValue, 0);

    byte[] oldSerializedValue = getBinaryValue(entryPosition, oldSize);

    if (ODefaultComparator.INSTANCE.compare(oldSerializedValue, newSerializedValue) == 0)
      return 0;

    setBinaryValue(entryPosition, newSerializedValue);
    return 1;
  }

  Entry<K, V> deleteEntry(int index) throws IOException {
    final Entry<K, V> removedEntry = getEntry(index);

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);

    final int positionOffset = POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE;
    final int entryPosition = getIntValue(positionOffset);

    final int keySize;
    if (encryption == null) {
      keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition + OLongSerializer.LONG_SIZE);
    } else {
      final int encryptedSize = getIntValue(entryPosition + OLongSerializer.LONG_SIZE);
      keySize = encryptedSize + OIntegerSerializer.INT_SIZE;
    }

    final int ridSize = getObjectSizeInDirectMemory(valueSerializer, entryPosition + keySize + OLongSerializer.LONG_SIZE);
    final int entrySize = keySize + ridSize + OLongSerializer.LONG_SIZE;

    moveData(positionOffset + OIntegerSerializer.INT_SIZE, positionOffset,
        size() * OIntegerSerializer.INT_SIZE - (index + 1) * OIntegerSerializer.INT_SIZE);

    if (entryPosition > freePointer)
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);

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

    return removedEntry;
  }

  public boolean addEntry(long hashCode, K key, V value) throws IOException {
    int entreeSize;

    byte[] encryptedKey = null;
    if (encryption == null) {
      entreeSize =
          keySerializer.getObjectSize(key, (Object[]) keyTypes) + valueSerializer.getObjectSize(value) + OLongSerializer.LONG_SIZE;
    } else {
      final int keySize = keySerializer.getObjectSize(key, (Object[]) keyTypes);
      final byte[] serializedKey = new byte[keySize];
      keySerializer.serializeNativeObject(key, serializedKey, 0, (Object[]) keyTypes);

      encryptedKey = encryption.encrypt(serializedKey);
      entreeSize =
          encryptedKey.length + OIntegerSerializer.INT_SIZE + valueSerializer.getObjectSize(value) + OLongSerializer.LONG_SIZE;
    }

    int freePointer = getIntValue(FREE_POINTER_OFFSET);

    int size = size();
    if (freePointer - entreeSize < POSITIONS_ARRAY_OFFSET + (size + 1) * OIntegerSerializer.INT_SIZE)
      return false;

    final int index = binarySearch(key, hashCode);
    if (index >= 0)
      throw new IllegalArgumentException("Given value is present in bucket.");

    final int insertionPoint = -index - 1;
    insertEntry(hashCode, key, value, insertionPoint, entreeSize, encryptedKey);

    return true;
  }

  private void insertEntry(long hashCode, K key, V value, int insertionPoint, int entreeSize, byte[] encryptedKey)
      throws IOException {
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    int size = size();

    final int positionsOffset = insertionPoint * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;

    moveData(positionsOffset, positionsOffset + OIntegerSerializer.INT_SIZE,
        size() * OIntegerSerializer.INT_SIZE - insertionPoint * OIntegerSerializer.INT_SIZE);

    final int entreePosition = freePointer - entreeSize;
    setIntValue(positionsOffset, entreePosition);
    serializeEntry(hashCode, key, value, entreePosition, encryptedKey);

    setIntValue(FREE_POINTER_OFFSET, entreePosition);
    setIntValue(SIZE_OFFSET, size + 1);
  }

  void appendEntry(long hashCode, K key, V value) throws IOException {
    final int positionsOffset = size() * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;
    final int entreeSize;
    byte[] encryptedKey = null;
    if (encryption == null) {
      entreeSize =
          keySerializer.getObjectSize(key, (Object[]) keyTypes) + valueSerializer.getObjectSize(value) + OLongSerializer.LONG_SIZE;
    } else {
      final int keySize = keySerializer.getObjectSize(key, (Object[]) keyTypes);
      final byte[] serializedKey = new byte[keySize];
      keySerializer.serializeNativeObject(key, serializedKey, 0, (Object[]) keyTypes);

      encryptedKey = encryption.encrypt(serializedKey);
      entreeSize =
          OIntegerSerializer.INT_SIZE + encryptedKey.length + valueSerializer.getObjectSize(value) + OLongSerializer.LONG_SIZE;
    }

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);
    final int entreePosition = freePointer - entreeSize;

    setIntValue(positionsOffset, entreePosition);
    serializeEntry(hashCode, key, value, entreePosition, encryptedKey);

    setIntValue(FREE_POINTER_OFFSET, freePointer - entreeSize);
    setIntValue(SIZE_OFFSET, size() + 1);
  }

  private void serializeEntry(long hashCode, K key, V value, int entryOffset, byte[] encryptedKey) throws IOException {
    setLongValue(entryOffset, hashCode);
    entryOffset += OLongSerializer.LONG_SIZE;

    if (encryptedKey == null) {
      final int keySize = keySerializer.getObjectSize(key, (Object[]) keyTypes);
      byte[] binaryKey = new byte[keySize];
      keySerializer.serializeNativeObject(key, binaryKey, 0, (Object[]) keyTypes);
      setBinaryValue(entryOffset, binaryKey);

      entryOffset += keySize;
    } else {
      setIntValue(entryOffset, encryptedKey.length);
      entryOffset += OIntegerSerializer.INT_SIZE;

      setBinaryValue(entryOffset, encryptedKey);
      entryOffset += encryptedKey.length;
    }

    final int valueSize = valueSerializer.getObjectSize(value);
    final byte[] binaryValue = new byte[valueSize];
    valueSerializer.serializeNativeObject(value, binaryValue, 0);

    setBinaryValue(entryOffset, binaryValue);
  }

  public int getDepth() {
    return getByteValue(DEPTH_OFFSET);
  }

  public void setDepth(int depth) {
    setByteValue(DEPTH_OFFSET, (byte) depth);
  }

  public long getNextRemovedBucketPair() {
    return getLongValue(NEXT_REMOVED_BUCKET_OFFSET);
  }

  public void setNextRemovedBucketPair(long nextRemovedBucketPair) throws IOException {
    setLongValue(NEXT_REMOVED_BUCKET_OFFSET, nextRemovedBucketPair);
  }

  public long getSplitHistory(int level) {
    return getLongValue(HISTORY_OFFSET + OLongSerializer.LONG_SIZE * level);
  }

  public void setSplitHistory(int level, long position) throws IOException {
    setLongValue(HISTORY_OFFSET + OLongSerializer.LONG_SIZE * level, position);
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
