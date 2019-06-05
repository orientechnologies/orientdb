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

package com.orientechnologies.orient.core.storage.index.sbtree.local.v2;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.orientechnologies.orient.core.storage.cluster.OClusterPage.PAGE_SIZE;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/7/13
 */
public class OSBTreeBucket<K, V> extends ODurablePage {
  private static final int FREE_POINTER_OFFSET  = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET          = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int IS_LEAF_OFFSET       = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int LEFT_SIBLING_OFFSET  = IS_LEAF_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int RIGHT_SIBLING_OFFSET = LEFT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int TREE_SIZE_OFFSET = RIGHT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  /**
   * KEY_SERIALIZER_OFFSET and VALUE_SERIALIZER_OFFSET are no longer used by sb-tree since 1.7. However we left them in buckets to
   * support backward compatibility.
   */
  private static final int KEY_SERIALIZER_OFFSET   = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int VALUE_SERIALIZER_OFFSET = KEY_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;

  private static final int FREE_VALUES_LIST_OFFSET = VALUE_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;

  private static final int POSITIONS_ARRAY_OFFSET = FREE_VALUES_LIST_OFFSET + OLongSerializer.LONG_SIZE;

  private final boolean isLeaf;

  private final OBinarySerializer<K> keySerializer;
  private final OBinarySerializer<V> valueSerializer;

  private final OType[] keyTypes;

  private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

  OSBTreeBucket(OCacheEntry cacheEntry, boolean isLeaf, OBinarySerializer<K> keySerializer, OType[] keyTypes,
      OBinarySerializer<V> valueSerializer) {
    super(cacheEntry);

    this.isLeaf = isLeaf;
    this.keySerializer = keySerializer;
    this.keyTypes = keyTypes;
    this.valueSerializer = valueSerializer;

    setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);
    setIntValue(SIZE_OFFSET, 0);

    setByteValue(IS_LEAF_OFFSET, (byte) (isLeaf ? 1 : 0));
    setLongValue(LEFT_SIBLING_OFFSET, -1);
    setLongValue(RIGHT_SIBLING_OFFSET, -1);

    setLongValue(TREE_SIZE_OFFSET, 0);
    setLongValue(FREE_VALUES_LIST_OFFSET, -1);

    setByteValue(KEY_SERIALIZER_OFFSET, this.keySerializer.getId());
    setByteValue(VALUE_SERIALIZER_OFFSET, this.valueSerializer.getId());
  }

  public OSBTreeBucket(OCacheEntry cacheEntry, OBinarySerializer<K> keySerializer, OType[] keyTypes,
      OBinarySerializer<V> valueSerializer) {
    super(cacheEntry);
    this.keyTypes = keyTypes;

    this.isLeaf = getByteValue(IS_LEAF_OFFSET) > 0;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public void setTreeSize(long size) {
    setLongValue(TREE_SIZE_OFFSET, size);
  }

  public long getTreeSize() {
    return getLongValue(TREE_SIZE_OFFSET);
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  long getValuesFreeListFirstIndex() {
    return getLongValue(FREE_VALUES_LIST_OFFSET);
  }

  void setValuesFreeListFirstIndex(long pageIndex) {
    setLongValue(FREE_VALUES_LIST_OFFSET, pageIndex);
  }

  public int find(K key) {
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

  public void remove(final int entryIndex, byte[] oldRawKey, byte[] oldRawValue) {
    final int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);
    final int keySize;

    if (oldRawKey == null) {
      keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      keySize = oldRawKey.length;
    }

    final int entrySize;
    if (isLeaf) {
      if (valueSerializer.isFixedLength()) {
        entrySize = keySize + valueSerializer.getFixedLength() + OByteSerializer.BYTE_SIZE;
      } else {
        assert getByteValue(entryPosition + keySize) == 0;
        entrySize = keySize + oldRawValue.length + OByteSerializer.BYTE_SIZE;
      }
    } else {
      throw new IllegalStateException("Remove is applies to leaf buckets only");
    }

    int size = getIntValue(SIZE_OFFSET);
    if (entryIndex < size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE, (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
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

  public int size() {
    return getIntValue(SIZE_OFFSET);
  }

  public SBTreeEntry<K, V> getEntry(int entryIndex) {
    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (isLeaf) {
      K key;

      key = deserializeFromDirectMemory(keySerializer, entryPosition);

      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);

      boolean isLinkValue = getByteValue(entryPosition) > 0;
      long link = -1;
      V value = null;

      if (isLinkValue)
        link = deserializeFromDirectMemory(OLongSerializer.INSTANCE, entryPosition + OByteSerializer.BYTE_SIZE);
      else
        value = deserializeFromDirectMemory(valueSerializer, entryPosition + OByteSerializer.BYTE_SIZE);

      return new SBTreeEntry<>(-1, -1, key, new OSBTreeValue<>(link >= 0, link, value));
    } else {
      long leftChild = getLongValue(entryPosition);
      entryPosition += OLongSerializer.LONG_SIZE;

      long rightChild = getLongValue(entryPosition);
      entryPosition += OLongSerializer.LONG_SIZE;

      K key = deserializeFromDirectMemory(keySerializer, entryPosition);

      return new SBTreeEntry<>(leftChild, rightChild, key, null);
    }
  }

  byte[] getRawEntry(final int entryIndex) {
    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    final int startEntryPosition = entryPosition;

    if (isLeaf) {
      final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);

      entryPosition += keySize;

      assert getByteValue(entryPosition) == 0;

      final int valueSize = getObjectSizeInDirectMemory(valueSerializer, entryPosition + OByteSerializer.BYTE_SIZE);

      return getBinaryValue(startEntryPosition, keySize + valueSize + OByteSerializer.BYTE_SIZE);
    } else {
      entryPosition += 2 * OLongSerializer.LONG_SIZE;

      final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);

      return getBinaryValue(startEntryPosition, keySize + 2 * OLongSerializer.LONG_SIZE);
    }
  }

  /**
   * Obtains the value stored under the given entry index in this bucket.
   *
   * @param entryIndex the value entry index.
   *
   * @return the obtained value.
   */
  public OSBTreeValue<V> getValue(int entryIndex) {
    assert isLeaf;

    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    // skip key

    entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);

    boolean isLinkValue = getByteValue(entryPosition) > 0;
    long link = -1;
    V value = null;

    if (isLinkValue)
      link = deserializeFromDirectMemory(OLongSerializer.INSTANCE, entryPosition + OByteSerializer.BYTE_SIZE);
    else
      value = deserializeFromDirectMemory(valueSerializer, entryPosition + OByteSerializer.BYTE_SIZE);

    return new OSBTreeValue<>(link >= 0, link, value);
  }

  byte[] getRawValue(final int entryIndex) {
    assert isLeaf;

    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    // skip key

    entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);

    assert getByteValue(entryPosition) == 0;

    final int valueSize = getObjectSizeInDirectMemory(valueSerializer, entryPosition + OByteSerializer.BYTE_SIZE);

    return getBinaryValue(entryPosition + OByteSerializer.BYTE_SIZE, valueSize);
  }

  byte[] getRawKey(final int entryIndex) {
    assert isLeaf;

    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    byte[] rawKey;

    final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
    rawKey = getBinaryValue(entryPosition, keySize);

    return rawKey;
  }

  public K getKey(int index) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf)
      entryPosition += 2 * OLongSerializer.LONG_SIZE;

    return deserializeFromDirectMemory(keySerializer, entryPosition);
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  public void addAll(final List<byte[]> rawEntries) {
    for (int i = 0; i < rawEntries.size(); i++) {
      appendRawEntry(i, rawEntries.get(i));
    }

    setIntValue(SIZE_OFFSET, rawEntries.size());
  }

  public void shrink(final int newSize) {
    final List<byte[]> rawEntries = new ArrayList<>(newSize);

    for (int i = 0; i < newSize; i++) {
      rawEntries.add(getRawEntry(i));
    }

    setIntValue(FREE_POINTER_OFFSET, PAGE_SIZE);

    int index = 0;
    for (final byte[] entry : rawEntries) {
      appendRawEntry(index, entry);
      index++;
    }

    setIntValue(SIZE_OFFSET, newSize);

  }

  boolean addLeafEntry(final int index, final byte[] serializedKey, final byte[] serializedValue) {
    final int entrySize = serializedKey.length + serializedValue.length + OByteSerializer.BYTE_SIZE;

    assert isLeaf;
    final int size = getIntValue(SIZE_OFFSET);

    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      return false;
    }

    if (index <= size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE, (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    setBinaryValue(freePointer, serializedKey);
    setByteValue(freePointer + serializedKey.length, (byte) 0);
    setBinaryValue(freePointer + serializedKey.length + OByteSerializer.BYTE_SIZE, serializedValue);

    return true;
  }

  private void appendRawEntry(final int index, final byte[] rawEntry) {
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    freePointer -= rawEntry.length;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);

    setBinaryValue(freePointer, rawEntry);
  }

  public boolean addEntry(int index, SBTreeEntry<K, V> treeEntry, boolean updateNeighbors) {
    final byte[] serializedKey = keySerializer.serializeNativeAsWhole(treeEntry.key, (Object[]) keyTypes);
    final int keySize = keySerializer.getObjectSize(treeEntry.key, (Object[]) keyTypes);

    int valueSize = 0;
    int entrySize = keySize;

    if (isLeaf) {
      if (valueSerializer.isFixedLength())
        valueSize = valueSerializer.getFixedLength();
      else {
        if (treeEntry.value.isLink())
          valueSize = OLongSerializer.LONG_SIZE;
        else
          valueSize = valueSerializer.getObjectSize(treeEntry.value.getValue());
      }

      entrySize += valueSize + OByteSerializer.BYTE_SIZE;
    } else
      entrySize += 2 * OLongSerializer.LONG_SIZE;

    int size = size();
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET)
      return false;

    if (index <= size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE, (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    if (isLeaf) {
      freePointer += setBinaryValue(freePointer, serializedKey);

      freePointer += setByteValue(freePointer, treeEntry.value.isLink() ? (byte) 1 : (byte) 0);

      byte[] serializedValue = new byte[valueSize];
      if (treeEntry.value.isLink())
        OLongSerializer.INSTANCE.serializeNative(treeEntry.value.getLink(), serializedValue, 0);
      else
        valueSerializer.serializeNativeObject(treeEntry.value.getValue(), serializedValue, 0);

      setBinaryValue(freePointer, serializedValue);
    } else {
      freePointer += setLongValue(freePointer, treeEntry.leftChild);
      freePointer += setLongValue(freePointer, treeEntry.rightChild);

      setBinaryValue(freePointer, serializedKey);

      size++;

      if (updateNeighbors && size > 1) {
        if (index < size - 1) {
          final int nextEntryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE);
          setLongValue(nextEntryPosition, treeEntry.rightChild);
        }

        if (index > 0) {
          final int prevEntryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + (index - 1) * OIntegerSerializer.INT_SIZE);
          setLongValue(prevEntryPosition + OLongSerializer.LONG_SIZE, treeEntry.leftChild);
        }
      }
    }

    return true;
  }

  void updateValue(final int index, final byte[] value) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);

    assert getByteValue(entryPosition) == 0;

    entryPosition += OByteSerializer.BYTE_SIZE;

    setBinaryValue(entryPosition, value);
  }

  public void setLeftSibling(long pageIndex) {
    setLongValue(LEFT_SIBLING_OFFSET, pageIndex);
  }

  public long getLeftSibling() {
    return getLongValue(LEFT_SIBLING_OFFSET);
  }

  public void setRightSibling(long pageIndex) {
    setLongValue(RIGHT_SIBLING_OFFSET, pageIndex);
  }

  public long getRightSibling() {
    return getLongValue(RIGHT_SIBLING_OFFSET);
  }

  public static final class SBTreeEntry<K, V> implements Comparable<SBTreeEntry<K, V>> {
    private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

    public final long            leftChild;
    public final long            rightChild;
    public final K               key;
    public final OSBTreeValue<V> value;

    public SBTreeEntry(long leftChild, long rightChild, K key, OSBTreeValue<V> value) {
      this.leftChild = leftChild;
      this.rightChild = rightChild;
      this.key = key;
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      final SBTreeEntry<?, ?> that = (SBTreeEntry<?, ?>) o;

      if (leftChild != that.leftChild)
        return false;
      if (rightChild != that.rightChild)
        return false;
      if (!key.equals(that.key))
        return false;
      if (value != null) {
        return value.equals(that.value);
      } else {
        return that.value == null;
      }

    }

    @Override
    public int hashCode() {
      int result = (int) (leftChild ^ (leftChild >>> 32));
      result = 31 * result + (int) (rightChild ^ (rightChild >>> 32));
      result = 31 * result + key.hashCode();
      result = 31 * result + (value != null ? value.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "SBTreeEntry{" + "leftChild=" + leftChild + ", rightChild=" + rightChild + ", key=" + key + ", value=" + value + '}';
    }

    @Override
    public int compareTo(SBTreeEntry<K, V> other) {
      return comparator.compare(key, other.key);
    }
  }
}
