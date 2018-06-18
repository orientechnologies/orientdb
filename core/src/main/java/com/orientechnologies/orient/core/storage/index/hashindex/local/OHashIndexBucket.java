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

import java.nio.ByteBuffer;
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

  public OHashIndexBucket(final OCacheEntry cacheEntry) {
    super(cacheEntry);

    keySerializer = null;
    valueSerializer = null;
    encryption = null;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  OHashIndexBucket(final int depth, final OCacheEntry cacheEntry, final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer, final OEncryption encryption) {
    super(cacheEntry);

    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.encryption = encryption;

    init(depth);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  OHashIndexBucket(final OCacheEntry cacheEntry, final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer, final OEncryption encryption) {
    super(cacheEntry);

    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.encryption = encryption;
  }

  public void init(final int depth) {
    buffer.put(DEPTH_OFFSET, (byte) depth);
    buffer.putInt(FREE_POINTER_OFFSET, MAX_BUCKET_SIZE_BYTES);
    buffer.putInt(SIZE_OFFSET, 0);
    cacheEntry.markDirty();
  }

  public Entry<K, V> find(final K key, final long hashCode) {
    final int index = binarySearch(key, hashCode);
    if (index < 0) {
      return null;
    }

    return getEntry(index);
  }

  private int binarySearch(final K key, final long hashCode) {
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

  private static boolean lessThanUnsigned(final long longOne, final long longTwo) {
    return (longOne + Long.MIN_VALUE) < (longTwo + Long.MIN_VALUE);
  }

  private static boolean greaterThanUnsigned(final long longOne, final long longTwo) {
    return (longOne + Long.MIN_VALUE) > (longTwo + Long.MIN_VALUE);
  }

  public Entry<K, V> getEntry(final int index) {
    int entryPosition = buffer.getInt(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    final long hashCode = buffer.getLong(entryPosition);
    entryPosition += OLongSerializer.LONG_SIZE;

    final K key;

    final ByteBuffer buffer = getBufferDuplicate();
    if (encryption == null) {
      buffer.position(entryPosition);
      key = keySerializer.deserializeFromByteBufferObject(buffer);
      buffer.position(entryPosition);
      entryPosition += keySerializer.getObjectSizeInByteBuffer(buffer);
    } else {
      final int encryptedSize = buffer.getInt(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final byte[] encryptedKey = new byte[encryptedSize];
      buffer.position(entryPosition);
      buffer.get(encryptedKey);

      entryPosition += encryptedSize;

      final byte[] serializedKey = encryption.decrypt(encryptedKey);
      key = keySerializer.deserializeNativeObject(serializedKey, 0);
    }

    buffer.position(entryPosition);
    final V value = valueSerializer.deserializeFromByteBufferObject(buffer);
    return new Entry<>(key, value, hashCode);
  }

  RawEntry getRawEntry(final int index) {
    int entryPosition = buffer.getInt(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    final long hashCode = buffer.getLong(entryPosition);
    entryPosition += OLongSerializer.LONG_SIZE;

    final ByteBuffer buffer = getBufferDuplicate();
    final byte[] rawKey;
    if (encryption == null) {
      buffer.position(entryPosition);
      final int keyLen = keySerializer.getObjectSizeInByteBuffer(buffer);

      buffer.position(entryPosition);
      rawKey = new byte[keyLen];
      buffer.get(rawKey);

      entryPosition += rawKey.length;
    } else {
      final int encryptedLen = buffer.getInt(entryPosition);

      rawKey = new byte[encryptedLen + OIntegerSerializer.INT_SIZE];
      buffer.position(entryPosition);
      buffer.get(rawKey);

      entryPosition += encryptedLen + OIntegerSerializer.INT_SIZE;
    }

    buffer.position(entryPosition);
    final int valueLen = valueSerializer.getObjectSizeInByteBuffer(buffer);

    final byte[] rawValue = new byte[valueLen];
    buffer.position(entryPosition);
    buffer.get(rawValue);

    return new RawEntry(hashCode, rawKey, rawValue);
  }

  /**
   * Obtains the value stored under the given index in this bucket.
   *
   * @param index the value index.
   *
   * @return the obtained value.
   */
  public V getValue(final int index) {
    int entryPosition = buffer.getInt(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    // skip hash code
    entryPosition += OLongSerializer.LONG_SIZE;

    final ByteBuffer buffer = getBufferDuplicate();

    // skip key
    if (encryption == null) {
      buffer.position(entryPosition);
      entryPosition += keySerializer.getObjectSizeInByteBuffer(buffer);
    } else {
      final int encryptedSize = buffer.getInt(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE + encryptedSize;
    }

    buffer.position(entryPosition);
    return valueSerializer.deserializeFromByteBufferObject(buffer);
  }

  byte[] getRawValue(final int index) {
    int entryPosition = buffer.getInt(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);
    // skip hash code
    entryPosition += OLongSerializer.LONG_SIZE;

    final ByteBuffer buffer = getBufferDuplicate();
    // skip key
    if (encryption == null) {
      buffer.position(entryPosition);
      entryPosition += keySerializer.getObjectSizeInByteBuffer(buffer);
    } else {
      final int encryptedSize = buffer.getInt(entryPosition);
      entryPosition += encryptedSize + OIntegerSerializer.INT_SIZE;
    }

    buffer.position(entryPosition);
    final int valSize = valueSerializer.getObjectSizeInByteBuffer(buffer);

    final byte[] value = new byte[valSize];
    buffer.position(entryPosition);
    buffer.get(value);

    return value;
  }

  private long getHashCode(final int index) {
    final int entryPosition = buffer.getInt(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);
    return buffer.getLong(entryPosition);
  }

  public K getKey(final int index) {
    int entryPosition = buffer.getInt(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    final ByteBuffer buffer = getBufferDuplicate();

    if (encryption == null) {
      buffer.position(entryPosition + OLongSerializer.LONG_SIZE);
      return keySerializer.deserializeFromByteBufferObject(buffer);
    }

    entryPosition += OLongSerializer.LONG_SIZE;
    final int encryptedSize = buffer.getInt(entryPosition);
    entryPosition += OIntegerSerializer.INT_SIZE;

    final byte[] encryptedKey = new byte[encryptedSize];
    buffer.position(entryPosition);
    buffer.get(encryptedKey);

    return keySerializer.deserializeNativeObject(encryption.decrypt(encryptedKey), 0);
  }

  public int getIndex(final long hashCode, final K key) {
    return binarySearch(key, hashCode);
  }

  public int size() {
    return buffer.getInt(SIZE_OFFSET);
  }

  public Iterator<Entry<K, V>> iterator() {
    return new EntryIterator(0);
  }

  public Iterator<Entry<K, V>> iterator(final int index) {
    return new EntryIterator(index);
  }

  public int getContentSize() {
    return POSITIONS_ARRAY_OFFSET + size() * OIntegerSerializer.INT_SIZE + (MAX_BUCKET_SIZE_BYTES - buffer
        .getInt(FREE_POINTER_OFFSET));
  }

  void updateEntry(final int index, final byte[] value) {
    int entryPosition = buffer.getInt(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);
    entryPosition += OLongSerializer.LONG_SIZE;

    if (encryption == null) {
      buffer.position(entryPosition);
      entryPosition += keySerializer.getObjectSizeInByteBuffer(buffer);
    } else {
      entryPosition += buffer.getInt(entryPosition) + OIntegerSerializer.INT_SIZE;
    }

    buffer.position(entryPosition);
    buffer.put(value);

    cacheEntry.markDirty();
  }

  void deleteEntry(final int index) {
    final int freePointer = buffer.getInt(FREE_POINTER_OFFSET);

    final int positionOffset = POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE;
    final int entryPosition = buffer.getInt(positionOffset);

    final int keySize;
    if (encryption == null) {
      buffer.position(entryPosition + OLongSerializer.LONG_SIZE);
      keySize = keySerializer.getObjectSizeInByteBuffer(buffer);
    } else {
      keySize = buffer.getInt(entryPosition + OLongSerializer.LONG_SIZE) + OIntegerSerializer.INT_SIZE;
    }

    buffer.position(entryPosition + keySize + OLongSerializer.LONG_SIZE);
    final int ridSize = valueSerializer.getObjectSizeInByteBuffer(buffer);
    final int entrySize = keySize + ridSize + OLongSerializer.LONG_SIZE;

    moveData(positionOffset + OIntegerSerializer.INT_SIZE, positionOffset,
        size() * OIntegerSerializer.INT_SIZE - (index + 1) * OIntegerSerializer.INT_SIZE);

    if (entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;
    final int size = size();
    for (int i = 0; i < size - 1; i++) {
      final int currentEntryPosition = buffer.getInt(currentPositionOffset);
      if (currentEntryPosition < entryPosition)
        buffer.putInt(currentPositionOffset, currentEntryPosition + entrySize);
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    buffer.putInt(FREE_POINTER_OFFSET, freePointer + entrySize);
    buffer.putInt(SIZE_OFFSET, size - 1);

    cacheEntry.markDirty();
  }

  int entryInsertionIndex(final long hashCode, final K key, final int keyLen, final int valueLen) {
    final int entreeSize = keyLen + valueLen + OLongSerializer.LONG_SIZE;
    final int freePointer = buffer.getInt(FREE_POINTER_OFFSET);

    final int size = size();
    if (freePointer - entreeSize < POSITIONS_ARRAY_OFFSET + (size + 1) * OIntegerSerializer.INT_SIZE) {
      return -1;
    }

    final int index = binarySearch(key, hashCode);
    if (index >= 0) {
      throw new IllegalArgumentException("Given value is present in bucket.");
    }

    return -index - 1;
  }

  void insertEntry(final long hashCode, final byte[] key, final byte[] value, final int insertionPoint) {
    final int entreeSize = key.length + value.length + OLongSerializer.LONG_SIZE;
    insertEntry(hashCode, key, value, insertionPoint, entreeSize);
  }

  private void insertEntry(final long hashCode, final byte[] key, final byte[] value, final int insertionPoint,
      final int entreeSize) {
    final int freePointer = buffer.getInt(FREE_POINTER_OFFSET);
    final int size = size();

    final int positionsOffset = insertionPoint * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;

    moveData(positionsOffset, positionsOffset + OIntegerSerializer.INT_SIZE,
        size() * OIntegerSerializer.INT_SIZE - insertionPoint * OIntegerSerializer.INT_SIZE);

    final int entreePosition = freePointer - entreeSize;
    buffer.putInt(positionsOffset, entreePosition);
    serializeEntry(hashCode, key, value, entreePosition);

    buffer.putInt(FREE_POINTER_OFFSET, entreePosition);
    buffer.putInt(SIZE_OFFSET, size + 1);
    cacheEntry.markDirty();
  }

  void appendEntry(final long hashCode, final byte[] key, final byte[] value) {
    final int positionsOffset = size() * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;
    final int entreeSize = key.length + value.length + OLongSerializer.LONG_SIZE;

    final int freePointer = buffer.getInt(FREE_POINTER_OFFSET);
    final int entreePosition = freePointer - entreeSize;

    buffer.putInt(positionsOffset, entreePosition);
    serializeEntry(hashCode, key, value, entreePosition);

    buffer.putInt(FREE_POINTER_OFFSET, freePointer - entreeSize);
    buffer.putInt(SIZE_OFFSET, size() + 1);
    cacheEntry.markDirty();
  }

  private void serializeEntry(final long hashCode, final byte[] key, final byte[] value, final int entryOffset) {
    buffer.position(entryOffset);
    buffer.putLong(hashCode);
    buffer.put(key);
    buffer.put(value);
  }

  public int getDepth() {
    return buffer.get(DEPTH_OFFSET);
  }

  public void setDepth(final int depth) {
    buffer.put(DEPTH_OFFSET, (byte) depth);
    cacheEntry.markDirty();
  }

  public static final class RawEntry {
    public final long   hashCode;
    final        byte[] rawKey;
    final        byte[] rawValue;

    RawEntry(final long hashCode, final byte[] rawKey, final byte[] rawValue) {
      this.hashCode = hashCode;
      this.rawKey = rawKey;
      this.rawValue = rawValue;
    }
  }

  public static class Entry<K, V> {
    public final K    key;
    public final V    value;
    public final long hashCode;

    public Entry(final K key, final V value, final long hashCode) {
      this.key = key;
      this.value = value;
      this.hashCode = hashCode;
    }
  }

  private final class EntryIterator implements Iterator<Entry<K, V>> {
    private int currentIndex;

    private EntryIterator(final int currentIndex) {
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
