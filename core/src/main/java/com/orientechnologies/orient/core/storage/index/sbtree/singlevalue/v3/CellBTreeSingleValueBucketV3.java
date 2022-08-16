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

package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/7/13
 */
public final class CellBTreeSingleValueBucketV3<K> extends ODurablePage {
  private static final int RID_SIZE = OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;

  private static final int FREE_POINTER_OFFSET = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int IS_LEAF_OFFSET = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int LEFT_SIBLING_OFFSET = IS_LEAF_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int RIGHT_SIBLING_OFFSET = LEFT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int NEXT_FREE_LIST_PAGE_OFFSET = NEXT_FREE_POSITION;

  private static final int POSITIONS_ARRAY_OFFSET =
      RIGHT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

  public CellBTreeSingleValueBucketV3(final OCacheEntry cacheEntry) {
    super(cacheEntry);
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

  public void init(boolean isLeaf) {
    setFreePointer(MAX_PAGE_SIZE_BYTES);
    setSize(0);

    setByteValue(IS_LEAF_OFFSET, (byte) (isLeaf ? 1 : 0));
    setLongValue(LEFT_SIBLING_OFFSET, -1);
    setLongValue(RIGHT_SIBLING_OFFSET, -1);
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public int find(final K key, final OBinarySerializer<K> keySerializer) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final K midVal = getKey(mid, keySerializer);
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

  public int removeLeafEntry(final int entryIndex, byte[] key) {
    final int entryPosition = getPointer(entryIndex);

    final int entrySize;
    if (isLeaf()) {
      entrySize = key.length + RID_SIZE;
    } else {
      throw new IllegalStateException("Remove is applies to leaf buckets only");
    }

    int[] pointers = getPointers();
    int size = pointers.length;
    int startChanging = size;
    if (entryIndex < size - 1) {
      for (int i = entryIndex + 1; i < size; i++) {
        if (pointers[i] < entryPosition) {
          pointers[i] += entrySize;
        }
      }
      setPointersOffset(entryIndex, pointers, entryIndex + 1);
      startChanging = entryIndex;
    }
    for (int i = 0; i < startChanging; i++) {
      if (pointers[i] < entryPosition) {
        setPointer(i, pointers[i] + entrySize);
      }
    }
    size--;
    setSize(size);

    final int freePointer = getFreePointer();
    if (size > 0 && entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    setFreePointer(freePointer + entrySize);

    return size;
  }

  private int getSize() {
    return getIntValue(SIZE_OFFSET);
  }

  public int removeNonLeafEntry(
      final int entryIndex,
      boolean removeLeftChildPointer,
      final OBinarySerializer<K> keySerializer) {
    if (isLeaf()) {
      throw new IllegalStateException("Remove is applied to non-leaf buckets only");
    }

    final int entryPosition = getPointer(entryIndex);
    final int keySize =
        getObjectSizeInDirectMemory(keySerializer, entryPosition + 2 * OIntegerSerializer.INT_SIZE);
    final byte[] key = getBinaryValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE, keySize);

    return removeNonLeafEntry(entryIndex, key, removeLeftChildPointer);
  }

  public int removeNonLeafEntry(
      final int entryIndex, final byte[] key, boolean removeLeftChildPointer) {
    if (isLeaf()) {
      throw new IllegalStateException("Remove is applied to non-leaf buckets only");
    }

    final int entryPosition = getPointer(entryIndex);
    final int entrySize = key.length + 2 * OIntegerSerializer.INT_SIZE;

    final int leftChild = getIntValue(entryPosition);
    final int rightChild = getIntValue(entryPosition + OIntegerSerializer.INT_SIZE);

    int[] pointers = getPointers();
    int size = pointers.length;
    int startChanging = size;
    if (entryIndex < size - 1) {
      for (int i = entryIndex + 1; i < size; i++) {
        if (pointers[i] < entryPosition) {
          pointers[i] += entrySize;
        }
      }
      setPointersOffset(entryIndex, pointers, entryIndex + 1);
      startChanging = entryIndex;
    }
    for (int i = 0; i < startChanging; i++) {
      if (pointers[i] < entryPosition) {
        setPointer(i, pointers[i] + entrySize);
      }
    }
    size--;
    setSize(size);

    final int freePointer = getFreePointer();
    if (size > 0 && entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    setFreePointer(freePointer + entrySize);

    if (size > 0) {
      final int childPointer = removeLeftChildPointer ? rightChild : leftChild;

      if (entryIndex > 0) {
        final int prevEntryPosition = getPointer(entryIndex - 1);
        setIntValue(prevEntryPosition + OIntegerSerializer.INT_SIZE, childPointer);
      }
      if (entryIndex < size) {
        final int nextEntryPosition = getPointer(entryIndex);
        setIntValue(nextEntryPosition, childPointer);
      }
    }

    return size;
  }

  public int[] getPointers() {
    int size = getSize();
    return getIntArray(POSITIONS_ARRAY_OFFSET, size);
  }

  public void setPointersOffset(int position, int[] pointers, int pointersOffset) {
    setIntArray(
        POSITIONS_ARRAY_OFFSET + position * OIntegerSerializer.INT_SIZE, pointers, pointersOffset);
  }

  public int size() {
    return getSize();
  }

  public CellBTreeSingleValueEntryV3<K> getEntry(
      final int entryIndex, final OBinarySerializer<K> keySerializer) {
    int entryPosition = getPointer(entryIndex);

    if (isLeaf()) {
      final K key;

      key = deserializeFromDirectMemory(keySerializer, entryPosition);

      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);

      final int clusterId = getShortValue(entryPosition);
      final long clusterPosition = getLongValue(entryPosition + OShortSerializer.SHORT_SIZE);

      return new CellBTreeSingleValueEntryV3<>(
          -1, -1, key, new ORecordId(clusterId, clusterPosition));
    } else {
      final int leftChild = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final int rightChild = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final K key = deserializeFromDirectMemory(keySerializer, entryPosition);

      return new CellBTreeSingleValueEntryV3<>(leftChild, rightChild, key, null);
    }
  }

  public int getLeft(final int entryIndex) {
    assert !isLeaf();

    final int entryPosition = getPointer(entryIndex);

    return getIntValue(entryPosition);
  }

  public int getRight(final int entryIndex) {
    assert !isLeaf();

    final int entryPosition = getPointer(entryIndex);

    return getIntValue(entryPosition + OIntegerSerializer.INT_SIZE);
  }

  public byte[] getRawEntry(final int entryIndex, final OBinarySerializer<K> keySerializer) {
    int entryPosition = getPointer(entryIndex);
    final int startEntryPosition = entryPosition;

    if (isLeaf()) {
      final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);

      return getBinaryValue(startEntryPosition, keySize + RID_SIZE);
    } else {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;

      final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);

      return getBinaryValue(startEntryPosition, keySize + 2 * OIntegerSerializer.INT_SIZE);
    }
  }

  /**
   * Obtains the value stored under the given entry index in this bucket.
   *
   * @param entryIndex the value entry index.
   * @return the obtained value.
   */
  public ORID getValue(final int entryIndex, final OBinarySerializer<K> keySerializer) {
    assert isLeaf();

    int entryPosition = getPointer(entryIndex);

    // skip key
    entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);

    final int clusterId = getShortValue(entryPosition);
    final long clusterPosition = getLongValue(entryPosition + OShortSerializer.SHORT_SIZE);

    return new ORecordId(clusterId, clusterPosition);
  }

  byte[] getRawValue(final int entryIndex, final OBinarySerializer<K> keySerializer) {
    assert isLeaf();
    assert entryIndex < getSize();

    int entryPosition = getPointer(entryIndex);

    // skip key
    entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);

    return getBinaryValue(entryPosition, RID_SIZE);
  }

  public K getKey(final int index, final OBinarySerializer<K> keySerializer) {
    assert index < getSize();
    int entryPosition = getPointer(index);

    if (!isLeaf()) {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;
    }

    return deserializeFromDirectMemory(keySerializer, entryPosition);
  }

  private int getPointer(final int index) {
    return getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
  }

  public byte[] getRawKey(final int index, final OBinarySerializer<K> keySerializer) {
    int entryPosition = getPointer(index);

    if (!isLeaf()) {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;
    }

    final int keyLen = getObjectSizeInDirectMemory(keySerializer, entryPosition);
    return getBinaryValue(entryPosition, keyLen);
  }

  public boolean isLeaf() {
    return getByteValue(IS_LEAF_OFFSET) > 0;
  }

  public void addAll(final List<byte[]> rawEntries, final OBinarySerializer<K> keySerializer) {
    final int currentSize = size();
    for (int i = 0; i < rawEntries.size(); i++) {
      appendRawEntry(i + currentSize, rawEntries.get(i));
    }

    setSize(rawEntries.size() + currentSize);
  }

  public void shrink(final int newSize, final OBinarySerializer<K> keySerializer) {
    final int currentSize = size();
    final List<byte[]> rawEntries = new ArrayList<>(newSize);
    final List<byte[]> removedEntries = new ArrayList<>(currentSize - newSize);

    for (int i = 0; i < newSize; i++) {
      rawEntries.add(getRawEntry(i, keySerializer));
    }

    for (int i = newSize; i < currentSize; i++) {
      removedEntries.add(getRawEntry(i, keySerializer));
    }

    setFreePointer(MAX_PAGE_SIZE_BYTES);

    for (int i = 0; i < newSize; i++) {
      appendRawEntry(i, rawEntries.get(i));
    }

    setSize(newSize);
  }

  private void setSize(final int newSize) {
    setIntValue(SIZE_OFFSET, newSize);
  }

  public boolean addLeafEntry(
      final int index, final byte[] serializedKey, final byte[] serializedValue) {
    final int entrySize = serializedKey.length + serializedValue.length;

    assert isLeaf();
    final int size = getSize();

    int freePointer = getFreePointer();
    if (doesOverflow(entrySize, 1)) {
      return false;
    }

    if (index <= size - 1) {
      shiftPointers(index, index + 1, size - index);
    }

    freePointer -= entrySize;

    setFreePointer(freePointer);
    setPointer(index, freePointer);
    setSize(size + 1);

    setBinaryValue(freePointer, serializedKey);
    setBinaryValue(freePointer + serializedKey.length, serializedValue);

    return true;
  }

  private void shiftPointers(final int index, final int indexTo, final int count) {
    moveData(
        POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
        POSITIONS_ARRAY_OFFSET + indexTo * OIntegerSerializer.INT_SIZE,
        count * OIntegerSerializer.INT_SIZE);
  }

  private int setPointer(final int index, int pointer) {
    return setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, pointer);
  }

  private int setFreePointer(int freePointer) {
    return setIntValue(FREE_POINTER_OFFSET, freePointer);
  }

  private int getFreePointer() {
    return getIntValue(FREE_POINTER_OFFSET);
  }

  private void appendRawEntry(final int index, final byte[] rawEntry) {
    int freePointer = getFreePointer();
    freePointer -= rawEntry.length;

    setFreePointer(freePointer);
    setPointer(index, freePointer);

    setBinaryValue(freePointer, rawEntry);
  }

  public boolean addNonLeafEntry(
      final int index, final int leftChildIndex, final int newRightChildIndex, final byte[] key) {
    assert !isLeaf();

    final int keySize = key.length;

    final int entrySize = keySize + 2 * OIntegerSerializer.INT_SIZE;

    int size = size();
    int freePointer = getFreePointer();
    if (doesOverflow(entrySize, 1)) {
      return false;
    }

    if (index <= size - 1) {
      shiftPointers(index, index + 1, size - index);
    }

    freePointer -= entrySize;

    setFreePointer(freePointer);
    setPointer(index, freePointer);
    setSize(size + 1);

    freePointer += setIntValue(freePointer, leftChildIndex);
    freePointer += setIntValue(freePointer, newRightChildIndex);

    setBinaryValue(freePointer, key);

    size++;

    if (size > 1) {
      if (index < size - 1) {
        final int nextEntryPosition = getPointer(index + 1);
        setIntValue(nextEntryPosition, newRightChildIndex);
      }
    }

    return true;
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  public boolean updateKey(
      final int entryIndex, final byte[] key, final OBinarySerializer<K> keySerializer) {
    if (isLeaf()) {
      throw new IllegalStateException("Update key is applied to non-leaf buckets only");
    }

    final int entryPosition = getPointer(entryIndex);
    final int keySize =
        getObjectSizeInDirectMemory(keySerializer, entryPosition + 2 * OIntegerSerializer.INT_SIZE);
    assert entryPosition + keySize < MAX_PAGE_SIZE_BYTES;
    if (key.length == keySize) {
      setBinaryValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE, key);
      return true;
    }

    int size = getSize();
    int freePointer = getFreePointer();

    if (doesOverflow(key.length - keySize, 0)) {
      return false;
    }

    final int entrySize = keySize + 2 * OIntegerSerializer.INT_SIZE;

    final int leftChildIndex = getIntValue(entryPosition);
    final int rightChildIndex = getIntValue(entryPosition + OIntegerSerializer.INT_SIZE);

    if (size > 0 && entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
      updatePointers(size, entryPosition, entrySize, entryIndex);
    }

    freePointer = freePointer - key.length + keySize;

    setFreePointer(freePointer);
    setPointer(entryIndex, freePointer);

    freePointer += setIntValue(freePointer, leftChildIndex);
    freePointer += setIntValue(freePointer, rightChildIndex);

    setBinaryValue(freePointer, key);

    return true;
  }

  public void updateValue(final int index, final byte[] value, int keyLenght) {
    int entryPosition = getPointer(index);
    if (!isLeaf()) {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;
    }
    setBinaryValue(entryPosition + keyLenght, value);
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

  public int getNextFreeListPage() {
    return getIntValue(NEXT_FREE_LIST_PAGE_OFFSET);
  }

  public void setNextFreeListPage(int nextFreeListPage) {
    setIntValue(NEXT_FREE_LIST_PAGE_OFFSET, nextFreeListPage);
  }

  public long getRightSibling() {
    return getLongValue(RIGHT_SIBLING_OFFSET);
  }

  private void updatePointers(int size, int basePosition, int shiftSize, int toIgnore) {
    int[] pointers = getIntArray(POSITIONS_ARRAY_OFFSET, size);
    for (int i = 0; i < size; i++) {
      if (toIgnore == i) continue;
      if (pointers[i] < basePosition) {
        setPointer(i, pointers[i] + shiftSize);
      }
    }
  }

  private boolean doesOverflow(int requiredDataSpace, int requirePointerSpace) {
    int size = getSize();
    int freePointer = getFreePointer();
    return freePointer - requiredDataSpace
        < (size + requirePointerSpace) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;
  }
}
