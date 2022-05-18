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
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashTable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 2/17/13
 */
public final class HashIndexBucketV2<K, V> extends ODurablePage {
  private static final int FREE_POINTER_OFFSET = NEXT_FREE_POSITION;
  private static final int DEPTH_OFFSET = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int SIZE_OFFSET = DEPTH_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int HISTORY_OFFSET = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int NEXT_REMOVED_BUCKET_OFFSET =
      HISTORY_OFFSET + OLongSerializer.LONG_SIZE * 64;
  private static final int POSITIONS_ARRAY_OFFSET =
      NEXT_REMOVED_BUCKET_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int MAX_BUCKET_SIZE_BYTES =
      OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  private final Comparator keyComparator = ODefaultComparator.INSTANCE;

  public HashIndexBucketV2(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init(int depth) {
    setByteValue(DEPTH_OFFSET, (byte) depth);
    setIntValue(FREE_POINTER_OFFSET, MAX_BUCKET_SIZE_BYTES);
    setIntValue(SIZE_OFFSET, 0);
  }

  public OHashTable.Entry<K, V> find(
      final K key,
      final long hashCode,
      final OEncryption encryption,
      final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer) {
    final int index = binarySearch(key, hashCode, encryption, keySerializer);
    if (index < 0) return null;

    return getEntry(index, encryption, keySerializer, valueSerializer);
  }

  private int binarySearch(
      K key, long hashCode, OEncryption encryption, OBinarySerializer<K> keySerializer) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;

      final long midHashCode = getHashCode(mid);
      final int cmp;
      if (lessThanUnsigned(midHashCode, hashCode)) cmp = -1;
      else if (greaterThanUnsigned(midHashCode, hashCode)) cmp = 1;
      else {
        final K midVal = getKey(mid, encryption, keySerializer);
        //noinspection unchecked
        cmp = keyComparator.compare(midVal, key);
      }

      if (cmp < 0) low = mid + 1;
      else if (cmp > 0) high = mid - 1;
      else return mid; // key found
    }
    return -(low + 1); // key not found.
  }

  private static boolean lessThanUnsigned(long longOne, long longTwo) {
    return (longOne + Long.MIN_VALUE) < (longTwo + Long.MIN_VALUE);
  }

  private static boolean greaterThanUnsigned(long longOne, long longTwo) {
    return (longOne + Long.MIN_VALUE) > (longTwo + Long.MIN_VALUE);
  }

  public OHashTable.Entry<K, V> getEntry(
      final int index,
      final OEncryption encryption,
      final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer) {
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
    return new OHashTable.Entry<>(key, value, hashCode);
  }

  public OHashTable.RawEntry getRawEntry(
      final int index,
      final OEncryption encryption,
      final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    final long hashCode = getLongValue(entryPosition);
    entryPosition += OLongSerializer.LONG_SIZE;

    final byte[] key;
    final byte[] value;
    if (encryption == null) {
      final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
      key = getBinaryValue(entryPosition, keySize);
      entryPosition += keySize;
    } else {
      final int encryptedLength = getIntValue(entryPosition);
      key = getBinaryValue(entryPosition, encryptedLength + OIntegerSerializer.INT_SIZE);
      entryPosition += encryptedLength + OIntegerSerializer.INT_SIZE;
    }

    final int valueSize = getObjectSizeInDirectMemory(valueSerializer, entryPosition);
    value = getBinaryValue(entryPosition, valueSize);
    return new OHashTable.RawEntry(key, value, hashCode);
  }

  public byte[] getRawValue(
      final int index, final int keySize, final OBinarySerializer<V> valueSerializer) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    // skip hash code and key
    entryPosition += OLongSerializer.LONG_SIZE + keySize;
    final int rawSize = getObjectSizeInDirectMemory(valueSerializer, entryPosition);
    return getBinaryValue(entryPosition, rawSize);
  }

  /**
   * Obtains the value stored under the given index in this bucket.
   *
   * @param index the value index.
   * @return the obtained value.
   */
  public V getValue(
      final int index,
      final OEncryption encryption,
      final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer) {
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

  public K getKey(
      final int index, final OEncryption encryption, final OBinarySerializer<K> keySerializer) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    if (encryption == null) {
      return deserializeFromDirectMemory(keySerializer, entryPosition + OLongSerializer.LONG_SIZE);
    } else {
      final int encryptedLength = getIntValue(entryPosition + OLongSerializer.LONG_SIZE);
      final byte[] encryptedBinaryKey =
          getBinaryValue(
              entryPosition + OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE,
              encryptedLength);
      final byte[] decryptedBinaryKey = encryption.decrypt(encryptedBinaryKey);
      return keySerializer.deserializeNativeObject(decryptedBinaryKey, 0);
    }
  }

  public int getIndex(
      final long hashCode,
      final K key,
      final OEncryption encryption,
      final OBinarySerializer<K> keySerializer) {
    return binarySearch(key, hashCode, encryption, keySerializer);
  }

  public int size() {
    return getIntValue(SIZE_OFFSET);
  }

  public Iterator<OHashTable.RawEntry> iterator(
      final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer,
      final OEncryption encryption) {
    return new RawEntryIterator(0, keySerializer, valueSerializer, encryption);
  }

  public Iterator<OHashTable.Entry<K, V>> iterator(
      int index,
      OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer,
      OEncryption encryption) {
    return new EntryIterator(index, keySerializer, valueSerializer, encryption);
  }

  public int getContentSize() {
    return POSITIONS_ARRAY_OFFSET
        + size() * OIntegerSerializer.INT_SIZE
        + (MAX_BUCKET_SIZE_BYTES - getIntValue(FREE_POINTER_OFFSET));
  }

  public int updateEntry(final int index, final byte[] value, final byte[] oldValue, int keySize) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);
    entryPosition += OLongSerializer.LONG_SIZE + keySize;

    if (oldValue.length != value.length) {
      return -1;
    }

    if (ODefaultComparator.INSTANCE.compare(oldValue, value) == 0) {
      return 0;
    }

    setBinaryValue(entryPosition, value);
    return 1;
  }

  public void deleteEntry(
      final int index, final long hashCode, final byte[] key, final byte[] value) {
    final int size = size();
    if (index < 0 || index >= size) {
      throw new IllegalStateException("Can not delete entry outside of border of the bucket");
    }

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);

    final int positionOffset = POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE;
    final int entryPosition = getIntValue(positionOffset);

    final int entrySize = key.length + value.length + OLongSerializer.LONG_SIZE;

    moveData(
        positionOffset + OIntegerSerializer.INT_SIZE,
        positionOffset,
        size() * OIntegerSerializer.INT_SIZE - (index + 1) * OIntegerSerializer.INT_SIZE);

    if (entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;
    for (int i = 0; i < size - 1; i++) {
      int currentEntryPosition = getIntValue(currentPositionOffset);
      if (currentEntryPosition < entryPosition)
        setIntValue(currentPositionOffset, currentEntryPosition + entrySize);
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    setIntValue(FREE_POINTER_OFFSET, freePointer + entrySize);
    setIntValue(SIZE_OFFSET, size - 1);
  }

  public boolean addEntry(final int index, long hashCode, byte[] key, byte[] value) {
    final int entreeSize = key.length + value.length + OLongSerializer.LONG_SIZE;
    int freePointer = getIntValue(FREE_POINTER_OFFSET);

    int size = size();
    if (index < 0 || index > size) {
      throw new IllegalStateException("Can not insert entry outside of border of bucket");
    }
    if (freePointer - entreeSize
        < POSITIONS_ARRAY_OFFSET + (size + 1) * OIntegerSerializer.INT_SIZE) {
      return false;
    }

    insertEntry(hashCode, key, value, index, entreeSize);
    return true;
  }

  private void insertEntry(
      long hashCode, byte[] key, byte[] value, int insertionPoint, int entreeSize) {
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    int size = size();

    final int positionsOffset =
        insertionPoint * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;

    moveData(
        positionsOffset,
        positionsOffset + OIntegerSerializer.INT_SIZE,
        size() * OIntegerSerializer.INT_SIZE - insertionPoint * OIntegerSerializer.INT_SIZE);

    final int entreePosition = freePointer - entreeSize;
    setIntValue(positionsOffset, entreePosition);
    serializeEntry(hashCode, key, value, entreePosition);

    setIntValue(FREE_POINTER_OFFSET, entreePosition);
    setIntValue(SIZE_OFFSET, size + 1);
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

  private final class EntryIterator implements Iterator<OHashTable.Entry<K, V>> {
    private int currentIndex;
    private final OBinarySerializer<K> keySerializer;
    private final OBinarySerializer<V> valueSerializer;
    private final OEncryption encryption;

    private EntryIterator(
        int currentIndex,
        OBinarySerializer<K> keySerializer,
        OBinarySerializer<V> valueSerializer,
        OEncryption encryption) {
      this.currentIndex = currentIndex;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
      this.encryption = encryption;
    }

    @Override
    public boolean hasNext() {
      return currentIndex < size();
    }

    @Override
    public OHashTable.Entry<K, V> next() {
      if (currentIndex >= size())
        throw new NoSuchElementException("Iterator was reached last element");

      final OHashTable.Entry<K, V> entry =
          getEntry(currentIndex, encryption, keySerializer, valueSerializer);
      currentIndex++;
      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove operation is not supported");
    }
  }

  private final class RawEntryIterator implements Iterator<OHashTable.RawEntry> {
    private int currentIndex;
    private final OBinarySerializer<K> keySerializer;
    private final OBinarySerializer<V> valueSerializer;
    private final OEncryption encryption;

    private RawEntryIterator(
        int currentIndex,
        OBinarySerializer<K> keySerializer,
        OBinarySerializer<V> valueSerializer,
        OEncryption encryption) {
      this.currentIndex = currentIndex;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
      this.encryption = encryption;
    }

    @Override
    public boolean hasNext() {
      return currentIndex < size();
    }

    @Override
    public OHashTable.RawEntry next() {
      if (currentIndex >= size())
        throw new NoSuchElementException("Iterator was reached last element");

      final OHashTable.RawEntry entry =
          getRawEntry(currentIndex, encryption, keySerializer, valueSerializer);
      currentIndex++;
      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove operation is not supported");
    }
  }
}
