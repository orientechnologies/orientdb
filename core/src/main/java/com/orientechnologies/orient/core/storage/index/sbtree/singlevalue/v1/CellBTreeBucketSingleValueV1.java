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

package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/7/13
 */
public final class CellBTreeBucketSingleValueV1<K> extends ODurablePage {
  private static final int RID_SIZE = OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;

  private static final int FREE_POINTER_OFFSET = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int IS_LEAF_OFFSET = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int LEFT_SIBLING_OFFSET = IS_LEAF_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int RIGHT_SIBLING_OFFSET = LEFT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int POSITIONS_ARRAY_OFFSET =
      RIGHT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

  public CellBTreeBucketSingleValueV1(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init(boolean isLeaf) {
    setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);
    setIntValue(SIZE_OFFSET, 0);

    setByteValue(IS_LEAF_OFFSET, (byte) (isLeaf ? 1 : 0));
    setLongValue(LEFT_SIBLING_OFFSET, -1);
    setLongValue(RIGHT_SIBLING_OFFSET, -1);
  }

  public void switchBucketType() {
    if (!isEmpty()) {
      throw new IllegalStateException(
          "Type of bucket can be changed only bucket if bucket is empty");
    }

    final boolean isLeaf = isLeaf();
    if (isLeaf) {
      setByteValue(IS_LEAF_OFFSET, (byte) 0);
    } else {
      setByteValue(IS_LEAF_OFFSET, (byte) 1);
    }
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public int find(
      final K key, final OBinarySerializer<K> keySerializer, final OEncryption encryption) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final K midVal = getKey(mid, encryption, keySerializer);
      final int cmp = comparator.compare(midVal, key);

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }

    return -(low + 1); // key not found.
  }

  public void removeLeafEntry(final int entryIndex, byte[] key, byte[] value) {
    final int entryPosition =
        getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);

    final int entrySize;
    if (isLeaf()) {
      entrySize = key.length + RID_SIZE;
    } else {
      throw new IllegalStateException("Remove is applies to leaf buckets only");
    }

    int size = getIntValue(SIZE_OFFSET);
    if (entryIndex < size - 1) {
      moveData(
          POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE,
          (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(SIZE_OFFSET, size);

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (size > 0 && entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    setIntValue(FREE_POINTER_OFFSET, freePointer + entrySize);

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      final int currentEntryPosition = getIntValue(currentPositionOffset);
      if (currentEntryPosition < entryPosition) {
        setIntValue(currentPositionOffset, currentEntryPosition + entrySize);
      }
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }
  }

  public void removeNonLeafEntry(final int entryIndex, final byte[] key, final int prevChild) {
    if (isLeaf()) {
      throw new IllegalStateException("Remove is applied to non-leaf buckets only");
    }

    final int entryPosition =
        getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);
    final int entrySize = key.length + 2 * OIntegerSerializer.INT_SIZE;
    int size = getIntValue(SIZE_OFFSET);

    if (entryIndex < size - 1) {
      moveData(
          POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE,
          (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(SIZE_OFFSET, size);

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (size > 0 && entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    setIntValue(FREE_POINTER_OFFSET, freePointer + entrySize);

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      final int currentEntryPosition = getIntValue(currentPositionOffset);
      if (currentEntryPosition < entryPosition) {
        setIntValue(currentPositionOffset, currentEntryPosition + entrySize);
      }
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    if (prevChild >= 0) {
      if (entryIndex > 0) {
        final int prevEntryPosition =
            getIntValue(POSITIONS_ARRAY_OFFSET + (entryIndex - 1) * OIntegerSerializer.INT_SIZE);
        setIntValue(prevEntryPosition + OIntegerSerializer.INT_SIZE, prevChild);
      }

      if (entryIndex < size) {
        final int nextEntryPosition =
            getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);
        setIntValue(nextEntryPosition, prevChild);
      }
    }
  }

  public int size() {
    return getIntValue(SIZE_OFFSET);
  }

  public SBTreeEntry<K> getEntry(
      final int entryIndex, OEncryption encryption, OBinarySerializer<K> keySerializer) {
    int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (isLeaf()) {
      final K key;
      if (encryption == null) {
        key = deserializeFromDirectMemory(keySerializer, entryPosition);

        entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
      } else {
        final int encryptionSize = getIntValue(entryPosition);
        entryPosition += OIntegerSerializer.INT_SIZE;

        final byte[] encryptedKey = getBinaryValue(entryPosition, encryptionSize);
        entryPosition += encryptedKey.length;

        final byte[] serializedKey = encryption.decrypt(encryptedKey);

        key = keySerializer.deserializeNativeObject(serializedKey, 0);
      }

      final int clusterId = getShortValue(entryPosition);
      final long clusterPosition = getLongValue(entryPosition + OShortSerializer.SHORT_SIZE);

      return new SBTreeEntry<>(-1, -1, key, new ORecordId(clusterId, clusterPosition));
    } else {
      final int leftChild = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final int rightChild = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final K key;

      if (encryption == null) {
        key = deserializeFromDirectMemory(keySerializer, entryPosition);
      } else {
        final int encryptionSize = getIntValue(entryPosition);
        entryPosition += OIntegerSerializer.INT_SIZE;

        final byte[] encryptedKey = getBinaryValue(entryPosition, encryptionSize);

        final byte[] serializedKey = encryption.decrypt(encryptedKey);

        key = keySerializer.deserializeNativeObject(serializedKey, 0);
      }

      return new SBTreeEntry<>(leftChild, rightChild, key, null);
    }
  }

  int getLeft(final int entryIndex) {
    assert !isLeaf();

    final int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    return getIntValue(entryPosition);
  }

  int getRight(final int entryIndex) {
    assert !isLeaf();

    final int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    return getIntValue(entryPosition + OIntegerSerializer.INT_SIZE);
  }

  public byte[] getRawEntry(
      final int entryIndex, boolean isEncrypted, OBinarySerializer<K> keySerializer) {
    int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    final int startEntryPosition = entryPosition;

    if (isLeaf()) {
      final int keySize;
      if (!isEncrypted) {
        keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
      } else {
        final int encryptedSize = getIntValue(entryPosition);
        keySize = OIntegerSerializer.INT_SIZE + encryptedSize;
      }

      return getBinaryValue(startEntryPosition, keySize + RID_SIZE);
    } else {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;

      final int keySize;
      if (!isEncrypted) {
        keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
      } else {
        final int encryptedSize = getIntValue(entryPosition);
        keySize = OIntegerSerializer.INT_SIZE + encryptedSize;
      }

      return getBinaryValue(startEntryPosition, keySize + 2 * OIntegerSerializer.INT_SIZE);
    }
  }

  /**
   * Obtains the value stored under the given entry index in this bucket.
   *
   * @param entryIndex the value entry index.
   * @return the obtained value.
   */
  public ORID getValue(
      final int entryIndex, OEncryption encryption, OBinarySerializer<K> keySerializer) {
    assert isLeaf();

    int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    // skip key
    if (encryption == null) {
      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedSize = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE + encryptedSize;
    }

    final int clusterId = getShortValue(entryPosition);
    final long clusterPosition = getLongValue(entryPosition + OShortSerializer.SHORT_SIZE);

    return new ORecordId(clusterId, clusterPosition);
  }

  public ORID getValue(final int entryIndex, final int keyLen) {
    assert isLeaf();

    int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    entryPosition += keyLen;

    final int clusterId = getShortValue(entryPosition);
    final long clusterPosition = getLongValue(entryPosition + OShortSerializer.SHORT_SIZE);

    return new ORecordId(clusterId, clusterPosition);
  }

  byte[] getRawValue(
      final int entryIndex, OEncryption encryption, OBinarySerializer<K> keySerializer) {
    assert isLeaf();

    int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    // skip key
    if (encryption == null) {
      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedSize = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE + encryptedSize;
    }

    return getBinaryValue(entryPosition, RID_SIZE);
  }

  public K getKey(final int index, OEncryption encryption, OBinarySerializer<K> keySerializer) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf()) {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;
    }

    if (encryption == null) {
      return deserializeFromDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedSize = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final byte[] encryptedKey = getBinaryValue(entryPosition, encryptedSize);
      final byte[] serializedKey = encryption.decrypt(encryptedKey);
      return keySerializer.deserializeNativeObject(serializedKey, 0);
    }
  }

  public boolean isLeaf() {
    return getByteValue(IS_LEAF_OFFSET) > 0;
  }

  public void addAll(
      final List<byte[]> rawEntries,
      final boolean isEncrypted,
      final OBinarySerializer<K> keySerializer) {
    final int currentSize = size();
    for (int i = 0; i < rawEntries.size(); i++) {
      appendRawEntry(i + currentSize, rawEntries.get(i));
    }

    setIntValue(SIZE_OFFSET, currentSize + rawEntries.size());
  }

  public void shrink(
      final int newSize, final boolean isEncrypted, final OBinarySerializer<K> keySerializer) {
    final int currentSize = size();
    final List<byte[]> rawEntries = new ArrayList<>(newSize);
    final List<byte[]> removedEntries = new ArrayList<>(currentSize - newSize);

    for (int i = 0; i < newSize; i++) {
      rawEntries.add(getRawEntry(i, isEncrypted, keySerializer));
    }

    for (int i = newSize; i < currentSize; i++) {
      removedEntries.add(getRawEntry(i, isEncrypted, keySerializer));
    }

    setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);

    for (int i = 0; i < newSize; i++) {
      appendRawEntry(i, rawEntries.get(i));
    }

    setIntValue(SIZE_OFFSET, newSize);
  }

  public boolean addLeafEntry(
      final int index, final byte[] serializedKey, final byte[] serializedValue) {
    final int entrySize = serializedKey.length + serializedValue.length;

    assert isLeaf();
    final int size = getIntValue(SIZE_OFFSET);

    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (freePointer - entrySize
        < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      return false;
    }

    if (index <= size - 1) {
      moveData(
          POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE,
          (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    setBinaryValue(freePointer, serializedKey);
    setBinaryValue(freePointer + serializedKey.length, serializedValue);
    return true;
  }

  private void appendRawEntry(final int index, final byte[] rawEntry) {
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    freePointer -= rawEntry.length;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);

    setBinaryValue(freePointer, rawEntry);
  }

  public boolean addNonLeafEntry(
      final int index,
      final int leftChild,
      final int rightChild,
      final byte[] key,
      final boolean updateNeighbors) {
    assert !isLeaf();

    final int keySize = key.length;

    final int entrySize = keySize + 2 * OIntegerSerializer.INT_SIZE;

    int size = size();
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (freePointer - entrySize
        < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      return false;
    }

    if (index <= size - 1) {
      moveData(
          POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE,
          (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    freePointer += setIntValue(freePointer, leftChild);
    freePointer += setIntValue(freePointer, rightChild);

    setBinaryValue(freePointer, key);

    size++;

    if (updateNeighbors && size > 1) {
      if (index < size - 1) {
        final int nextEntryPosition =
            getIntValue(POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE);
        setIntValue(nextEntryPosition, rightChild);
      }

      if (index > 0) {
        final int prevEntryPosition =
            getIntValue(POSITIONS_ARRAY_OFFSET + (index - 1) * OIntegerSerializer.INT_SIZE);
        setIntValue(prevEntryPosition + OIntegerSerializer.INT_SIZE, leftChild);
      }
    }

    return true;
  }

  public void updateValue(final int index, final byte[] value, final int keySize) {
    final int entryPosition =
        getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) + keySize;

    setBinaryValue(entryPosition, value);
  }

  public void setLeftSibling(final long pageIndex) {
    setLongValue(LEFT_SIBLING_OFFSET, pageIndex);
  }

  public long getLeftSibling() {
    return getLongValue(LEFT_SIBLING_OFFSET);
  }

  public void setRightSibling(final long pageIndex) {
    setLongValue(RIGHT_SIBLING_OFFSET, pageIndex);
  }

  public long getRightSibling() {
    return getLongValue(RIGHT_SIBLING_OFFSET);
  }

  public static final class SBTreeEntry<K> implements Comparable<SBTreeEntry<K>> {
    private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

    protected final int leftChild;
    protected final int rightChild;
    public final K key;
    public final ORID value;

    public SBTreeEntry(final int leftChild, final int rightChild, final K key, final ORID value) {
      this.leftChild = leftChild;
      this.rightChild = rightChild;
      this.key = key;
      this.value = value;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final SBTreeEntry<?> that = (SBTreeEntry<?>) o;
      return leftChild == that.leftChild
          && rightChild == that.rightChild
          && Objects.equals(key, that.key)
          && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(leftChild, rightChild, key, value);
    }

    @Override
    public String toString() {
      return "CellBTreeEntry{"
          + "leftChild="
          + leftChild
          + ", rightChild="
          + rightChild
          + ", key="
          + key
          + ", value="
          + value
          + '}';
    }

    @Override
    public int compareTo(final SBTreeEntry<K> other) {
      return comparator.compare(key, other.key);
    }
  }
}
