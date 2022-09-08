package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.IntSerializer;
import java.util.ArrayList;
import java.util.List;

final class Bucket extends ODurablePage {

  private static final int FREE_POINTER_OFFSET = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int IS_LEAF_OFFSET = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int LEFT_SIBLING_OFFSET = IS_LEAF_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int RIGHT_SIBLING_OFFSET = LEFT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int POSITIONS_ARRAY_OFFSET =
      RIGHT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  public Bucket(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init(boolean isLeaf) {
    setFreePointer(MAX_PAGE_SIZE_BYTES);
    setSize(0);

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

  public int size() {
    return getSize();
  }

  public boolean isLeaf() {
    return getByteValue(IS_LEAF_OFFSET) > 0;
  }

  public int find(final EdgeKey key) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final EdgeKey midKey = getKey(mid);
      final int cmp = midKey.compareTo(key);

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

  public EdgeKey getKey(final int index) {
    int entryPosition = getPointer(index);

    if (!isLeaf()) {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;
    }

    return deserializeFromDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);
  }

  private byte[] getRawKey(final int index) {
    int entryPosition = getPointer(index);

    if (!isLeaf()) {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;
    }

    final int keySize = getObjectSizeInDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);
    return getBinaryValue(entryPosition, keySize);
  }

  public void removeLeafEntry(final int entryIndex, final int keySize, final int valueSize) {
    final int entryPosition = getPointer(entryIndex);

    final int entrySize;
    if (isLeaf()) {
      entrySize = keySize + valueSize;
    } else {
      throw new IllegalStateException("Remove is applies to leaf buckets only");
    }

    final int freePointer = getFreePointer();
    assert freePointer <= ODurablePage.MAX_PAGE_SIZE_BYTES;
    assert freePointer + entrySize <= ODurablePage.MAX_PAGE_SIZE_BYTES;

    int size = getSize();
    int[] pointers = getPointers();
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

    if (size > 0 && entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    setFreePointer(freePointer + entrySize);
  }

  public void removeNonLeafEntry(final int entryIndex, final byte[] key, final int prevChild) {
    if (isLeaf()) {
      throw new IllegalStateException("Remove is applied to non-leaf buckets only");
    }

    final int entryPosition = getPointer(entryIndex);
    final int entrySize = key.length + 2 * OIntegerSerializer.INT_SIZE;
    int size = getSize();

    final int leftChild = getIntValue(entryPosition);
    final int rightChild = getIntValue(entryPosition + OIntegerSerializer.INT_SIZE);

    int[] pointers = getPointers();
    int endSize = size - 1;
    if (entryIndex < size - 1) {
      for (int i = entryIndex + 1; i < size; i++) {
        if (pointers[i] < entryPosition) {
          pointers[i] += entrySize;
        }
      }
      setPointersOffset(entryIndex, pointers, entryIndex + 1);
      endSize = entryIndex;
    }

    size--;
    setSize(size);

    for (int i = 0; i < endSize; i++) {
      if (pointers[i] < entryPosition) {
        setPointer(i, pointers[i] + entrySize);
      }
    }

    final int freePointer = getFreePointer();
    assert freePointer <= ODurablePage.MAX_PAGE_SIZE_BYTES;

    if (size > 0 && entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    assert freePointer + entrySize <= ODurablePage.MAX_PAGE_SIZE_BYTES;
    setFreePointer(freePointer + entrySize);

    if (prevChild >= 0) {
      if (entryIndex > 0) {
        final int prevEntryPosition = getPointer(entryIndex - 1);
        setIntValue(prevEntryPosition + OIntegerSerializer.INT_SIZE, prevChild);
      }

      if (entryIndex < size) {
        final int nextEntryPosition = getPointer(entryIndex);
        setIntValue(nextEntryPosition, prevChild);
      }
    }
  }

  public TreeEntry getEntry(final int entryIndex) {
    int entryPosition = getPointer(entryIndex);

    if (isLeaf()) {
      final EdgeKey key;

      key = deserializeFromDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);

      entryPosition += getObjectSizeInDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);

      final int value = deserializeFromDirectMemory(IntSerializer.INSTANCE, entryPosition);

      return new TreeEntry(-1, -1, key, value);
    } else {
      final int leftChild = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final int rightChild = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final EdgeKey key = deserializeFromDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);

      return new TreeEntry(leftChild, rightChild, key, -1);
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

  public int getValue(final int entryIndex) {
    assert isLeaf();

    int entryPosition = getPointer(entryIndex);

    // skip key
    entryPosition += getObjectSizeInDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);
    return deserializeFromDirectMemory(IntSerializer.INSTANCE, entryPosition);
  }

  byte[] getRawValue(final int entryIndex) {
    assert isLeaf();

    int entryPosition = getPointer(entryIndex);

    // skip key
    entryPosition += getObjectSizeInDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);

    final int intSize = getObjectSizeInDirectMemory(IntSerializer.INSTANCE, entryPosition);
    return getBinaryValue(entryPosition, intSize);
  }

  public void addAll(final List<byte[]> rawEntries) {
    final int currentSize = size();
    for (int i = 0; i < rawEntries.size(); i++) {
      appendRawEntry(i + currentSize, rawEntries.get(i));
    }

    setSize(rawEntries.size() + currentSize);
  }

  private void appendRawEntry(final int index, final byte[] rawEntry) {
    int freePointer = getFreePointer();
    assert freePointer <= ODurablePage.MAX_PAGE_SIZE_BYTES;

    freePointer -= rawEntry.length;

    assert freePointer <= ODurablePage.MAX_PAGE_SIZE_BYTES;

    setFreePointer(freePointer);
    setPointer(index, freePointer);

    setBinaryValue(freePointer, rawEntry);
  }

  public void shrink(final int newSize) {
    final List<byte[]> rawEntries = new ArrayList<>(newSize);

    for (int i = 0; i < newSize; i++) {
      rawEntries.add(getRawEntry(i));
    }

    setFreePointer(MAX_PAGE_SIZE_BYTES);

    for (int i = 0; i < newSize; i++) {
      appendRawEntry(i, rawEntries.get(i));
    }

    setSize(newSize);
  }

  public byte[] getRawEntry(final int entryIndex) {
    int entryPosition = getPointer(entryIndex);
    final int startEntryPosition = entryPosition;

    if (isLeaf()) {
      final int keySize = getObjectSizeInDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);
      final int valueSize =
          getObjectSizeInDirectMemory(IntSerializer.INSTANCE, startEntryPosition + keySize);
      return getBinaryValue(startEntryPosition, keySize + valueSize);
    } else {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;

      final int keySize = getObjectSizeInDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);

      return getBinaryValue(startEntryPosition, keySize + 2 * OIntegerSerializer.INT_SIZE);
    }
  }

  public boolean addLeafEntry(
      final int index, final byte[] serializedKey, final byte[] serializedValue) {
    final int entrySize = serializedKey.length + serializedValue.length;

    assert isLeaf();
    final int size = getSize();

    int freePointer = getFreePointer();
    assert freePointer <= ODurablePage.MAX_PAGE_SIZE_BYTES;

    if (doesOverflow(entrySize, 1)) {
      return false;
    }

    if (index <= size - 1) {
      shiftPointers(index, index + 1, size - index);
    }

    freePointer -= entrySize;

    assert freePointer <= ODurablePage.MAX_PAGE_SIZE_BYTES;

    setFreePointer(freePointer);
    setPointer(index, freePointer);
    setSize(size + 1);

    setBinaryValue(freePointer, serializedKey);
    setBinaryValue(freePointer + serializedKey.length, serializedValue);

    return true;
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
    int freePointer = getFreePointer();
    assert freePointer <= ODurablePage.MAX_PAGE_SIZE_BYTES;

    if (doesOverflow(entrySize, 1)) {
      return false;
    }

    if (index <= size - 1) {
      shiftPointers(index, index + 1, size - index);
    }

    freePointer -= entrySize;

    assert freePointer <= ODurablePage.MAX_PAGE_SIZE_BYTES;

    setFreePointer(freePointer);
    setPointer(index, freePointer);
    setSize(size + 1);

    freePointer += setIntValue(freePointer, leftChild);
    freePointer += setIntValue(freePointer, rightChild);

    setBinaryValue(freePointer, key);

    size++;

    if (updateNeighbors && size > 1) {
      if (index < size - 1) {
        final int nextEntryPosition = getPointer(index + 1);
        setIntValue(nextEntryPosition, rightChild);
      }

      if (index > 0) {
        final int prevEntryPosition = getPointer(index - 1);
        setIntValue(prevEntryPosition + OIntegerSerializer.INT_SIZE, leftChild);
      }
    }

    return true;
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

  public void updateValue(final int index, final byte[] value, final int keySize) {
    final int entryPosition = getPointer(index) + keySize;

    final int valueSize = getObjectSizeInDirectMemory(IntSerializer.INSTANCE, entryPosition);
    if (valueSize == value.length) {
      setBinaryValue(entryPosition, value);
    } else {
      final byte[] rawKey = getRawKey(index);

      removeLeafEntry(index, keySize, valueSize);
      addLeafEntry(index, rawKey, value);
    }
  }

  public void setFreePointer(int value) {
    setIntValue(FREE_POINTER_OFFSET, value);
  }

  public int getFreePointer() {
    return getIntValue(FREE_POINTER_OFFSET);
  }

  public int getSize() {
    return getIntValue(SIZE_OFFSET);
  }

  public void setSize(int value) {
    setIntValue(SIZE_OFFSET, value);
  }

  public int getPointer(final int index) {
    return getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
  }

  public int[] getPointers() {
    int size = getSize();
    return getIntArray(POSITIONS_ARRAY_OFFSET, size);
  }

  public void setPointer(final int index, int value) {
    setIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET, value);
  }

  public void setPointersOffset(int position, int[] pointers, int pointersOffset) {
    setIntArray(
        POSITIONS_ARRAY_OFFSET + position * OIntegerSerializer.INT_SIZE, pointers, pointersOffset);
  }

  public void shiftPointers(int from, int to, int size) {
    moveData(
        POSITIONS_ARRAY_OFFSET + from * OIntegerSerializer.INT_SIZE,
        POSITIONS_ARRAY_OFFSET + to * OIntegerSerializer.INT_SIZE,
        size * OIntegerSerializer.INT_SIZE);
  }

  private boolean doesOverflow(int requiredDataSpace, int requirePointerSpace) {
    int size = getSize();
    int freePointer = getFreePointer();
    return freePointer - requiredDataSpace
        < (size + requirePointerSpace) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;
  }
}
