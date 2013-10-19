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

package com.orientechnologies.orient.core.index.sbtreebonsai.local;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTreeException;

/**
 * @author Andrey Lomakin
 * @since 8/7/13
 */
public class OSBTreeBonsaiBucket<K, V> extends OBonsaiBucketAbstract {
  /**
   * Maximum size of key-value pair which can be put in SBTreeBonsai in bytes (24576000 by default)
   */
  private static final int            MAX_ENTREE_SIZE          = 24576000;

  private static final int            FREE_POINTER_OFFSET      = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int            SIZE_OFFSET              = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int            IS_LEAF_OFFSET           = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int            FREE_LIST_POINTER_OFFSET = IS_LEAF_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int            LEFT_SIBLING_OFFSET      = FREE_LIST_POINTER_OFFSET + OBonsaiBucketPointer.SIZE;
  private static final int            RIGHT_SIBLING_OFFSET     = LEFT_SIBLING_OFFSET + OBonsaiBucketPointer.SIZE;

  private static final int            TREE_SIZE_OFFSET         = RIGHT_SIBLING_OFFSET + OBonsaiBucketPointer.SIZE;

  private static final int            KEY_SERIALIZER_OFFSET    = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int            VALUE_SERIALIZER_OFFSET  = KEY_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;

  private static final int            POSITIONS_ARRAY_OFFSET   = VALUE_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;

  public static final int             MAX_BUCKET_SIZE_BYTES    = OGlobalConfiguration.SBTREEBONSAI_BUCKET_SIZE.getValueAsInteger() * 1024;

  private final boolean               isLeaf;
  private final int                   offset;

  private final OBinarySerializer<K>  keySerializer;
  private final OBinarySerializer<V>  valueSerializer;

  private final Comparator<? super K> comparator               = ODefaultComparator.INSTANCE;

  public OSBTreeBonsaiBucket(ODirectMemoryPointer cachePointer, int pageOffset, boolean isLeaf, OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer, TrackMode trackMode) throws IOException {
    super(cachePointer, trackMode);

    this.offset = pageOffset;
    this.isLeaf = isLeaf;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;

    setIntValue(offset + FREE_POINTER_OFFSET, MAX_BUCKET_SIZE_BYTES);
    setIntValue(offset + SIZE_OFFSET, 0);

    setByteValue(offset + IS_LEAF_OFFSET, (byte) (isLeaf ? 1 : 0));
    setLongValue(offset + LEFT_SIBLING_OFFSET, -1);
    setLongValue(offset + RIGHT_SIBLING_OFFSET, -1);

    setLongValue(offset + TREE_SIZE_OFFSET, 0);

    setByteValue(offset + KEY_SERIALIZER_OFFSET, keySerializer.getId());
    setByteValue(offset + VALUE_SERIALIZER_OFFSET, valueSerializer.getId());
  }

  public OSBTreeBonsaiBucket(ODirectMemoryPointer cachePointer, int pageOffset, OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer, TrackMode trackMode) {
    super(cachePointer, trackMode);

    this.offset = pageOffset;
    this.isLeaf = getByteValue(offset + IS_LEAF_OFFSET) > 0;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public byte getKeySerializerId() {
    return getByteValue(offset + KEY_SERIALIZER_OFFSET);
  }

  public void setKeySerializerId(byte keySerializerId) {
    setByteValue(offset + KEY_SERIALIZER_OFFSET, keySerializerId);
  }

  public byte getValueSerializerId() {
    return getByteValue(offset + VALUE_SERIALIZER_OFFSET);
  }

  public void setValueSerializerId(byte valueSerializerId) {
    setByteValue(offset + VALUE_SERIALIZER_OFFSET, valueSerializerId);
  }

  public void setTreeSize(long size) throws IOException {
    setLongValue(offset + TREE_SIZE_OFFSET, size);
  }

  public long getTreeSize() {
    return getLongValue(offset + TREE_SIZE_OFFSET);
  }

  public boolean isEmpty() {
    return size() == 0;
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

  public void remove(int entryIndex) throws IOException {
    int entryPosition = getIntValue(offset + POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);

    int entrySize = keySerializer.getObjectSizeInDirectMemory(pagePointer, offset + entryPosition);
    if (isLeaf) {
      if (valueSerializer.isFixedLength())
        entrySize += valueSerializer.getFixedLength();
      else
        entrySize += valueSerializer.getObjectSizeInDirectMemory(pagePointer, offset + entryPosition + entrySize);
    } else {
      throw new IllegalStateException("Remove is applies to leaf buckets only");
    }

    int size = size();
    if (entryIndex < size - 1) {
      copyData(offset + POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE, offset + POSITIONS_ARRAY_OFFSET
          + entryIndex * OIntegerSerializer.INT_SIZE, (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(offset + SIZE_OFFSET, size);

    int freePointer = getIntValue(offset + FREE_POINTER_OFFSET);
    if (size > 0 && entryPosition > freePointer) {
      copyData(offset + freePointer, offset + freePointer + entrySize, entryPosition - freePointer);
    }
    setIntValue(offset + FREE_POINTER_OFFSET, freePointer + entrySize);

    int currentPositionOffset = offset + POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      int currentEntryPosition = getIntValue(currentPositionOffset);
      if (currentEntryPosition < entryPosition)
        setIntValue(currentPositionOffset, currentEntryPosition + entrySize);
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }
  }

  public int size() {
    return getIntValue(offset + SIZE_OFFSET);
  }

  public SBTreeEntry<K, V> getEntry(int entryIndex) {
    int entryPosition = getIntValue(offset + entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (isLeaf) {
      K key = keySerializer.deserializeFromDirectMemory(pagePointer, offset + entryPosition);
      entryPosition += keySerializer.getObjectSizeInDirectMemory(pagePointer, offset + entryPosition);

      V value = valueSerializer.deserializeFromDirectMemory(pagePointer, offset + entryPosition);

      return new SBTreeEntry<K, V>(OBonsaiBucketPointer.NULL, OBonsaiBucketPointer.NULL, key, value);
    } else {
      OBonsaiBucketPointer leftChild = getBucketPointer(offset + entryPosition);
      entryPosition += OBonsaiBucketPointer.SIZE;

      OBonsaiBucketPointer rightChild = getBucketPointer(offset + entryPosition);
      entryPosition += OBonsaiBucketPointer.SIZE;

      K key = keySerializer.deserializeFromDirectMemory(pagePointer, offset + entryPosition);

      return new SBTreeEntry<K, V>(leftChild, rightChild, key, null);
    }
  }

  public K getKey(int index) {
    int entryPosition = getIntValue(offset + index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf)
      entryPosition += 2 * (OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE);

    return keySerializer.deserializeFromDirectMemory(pagePointer, offset + entryPosition);
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  public void addAll(List<SBTreeEntry<K, V>> entries) throws IOException {
    for (int i = 0; i < entries.size(); i++)
      addEntry(i, entries.get(i), false);
  }

  public void shrink(int newSize) throws IOException {
    List<SBTreeEntry<K, V>> treeEntries = new ArrayList<SBTreeEntry<K, V>>(newSize);

    for (int i = 0; i < newSize; i++) {
      treeEntries.add(getEntry(i));
    }

    setIntValue(offset + FREE_POINTER_OFFSET, MAX_BUCKET_SIZE_BYTES);
    setIntValue(offset + SIZE_OFFSET, 0);

    int index = 0;
    for (SBTreeEntry<K, V> entry : treeEntries) {
      addEntry(index, entry, false);
      index++;
    }
  }

  public boolean addEntry(int index, SBTreeEntry<K, V> treeEntry, boolean updateNeighbors) throws IOException {
    final int keySize = keySerializer.getObjectSize(treeEntry.key);
    int valueSize = 0;
    int entrySize = keySize;

    if (isLeaf) {
      if (valueSerializer.isFixedLength())
        valueSize = valueSerializer.getFixedLength();
      else
        valueSize = valueSerializer.getObjectSize(treeEntry.value);

      entrySize += valueSize;

      checkEntreeSize(entrySize);
    } else
      entrySize += 2 * (OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE);

    int size = size();
    int freePointer = getIntValue(offset + FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET)
      return false;

    if (index <= size - 1) {
      copyData(offset + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, offset + POSITIONS_ARRAY_OFFSET + (index + 1)
          * OIntegerSerializer.INT_SIZE, (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(offset + FREE_POINTER_OFFSET, freePointer);
    setIntValue(offset + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(offset + SIZE_OFFSET, size + 1);

    if (isLeaf) {
      byte[] serializedKey = new byte[keySize];
      keySerializer.serializeNative(treeEntry.key, serializedKey, 0);

      setBinaryValue(offset + freePointer, serializedKey);
      freePointer += keySize;

      byte[] serializedValue = new byte[valueSize];
      valueSerializer.serializeNative(treeEntry.value, serializedValue, 0);
      setBinaryValue(offset + freePointer, serializedValue);

    } else {
      setBucketPointer(offset + freePointer, treeEntry.leftChild);
      freePointer += OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;

      setBucketPointer(offset + freePointer, treeEntry.rightChild);
      freePointer += OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;

      byte[] serializedKey = new byte[keySize];
      keySerializer.serializeNative(treeEntry.key, serializedKey, 0);
      setBinaryValue(offset + freePointer, serializedKey);

      size++;

      if (updateNeighbors && size > 1) {
        if (index < size - 1) {
          final int nextEntryPosition = getIntValue(offset + POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE);
          setBucketPointer(offset + nextEntryPosition, treeEntry.rightChild);
        }

        if (index > 0) {
          final int prevEntryPosition = getIntValue(offset + POSITIONS_ARRAY_OFFSET + (index - 1) * OIntegerSerializer.INT_SIZE);
          setBucketPointer(offset + prevEntryPosition + OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE,
              treeEntry.leftChild);
        }
      }
    }

    return true;
  }

  public boolean updateValue(int index, V value) throws IOException {
    if (valueSerializer.isFixedLength()) {
      int entryPosition = getIntValue(offset + index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

      entryPosition += keySerializer.getObjectSizeInDirectMemory(pagePointer, offset + entryPosition);

      byte[] serializedValue = new byte[valueSerializer.getFixedLength()];
      valueSerializer.serializeNative(value, serializedValue, 0);

      setBinaryValue(offset + entryPosition, serializedValue);
      return true;
    }

    final int entryPosition = getIntValue(offset + index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    int entreeSize = keySerializer.getObjectSizeInDirectMemory(pagePointer, offset + entryPosition);
    entreeSize += valueSerializer.getObjectSize(value);

    checkEntreeSize(entreeSize);

    final K key = getKey(index);
    remove(index);
    return addEntry(index, new SBTreeEntry<K, V>(OBonsaiBucketPointer.NULL, OBonsaiBucketPointer.NULL, key, value), false);
  }

  private void checkEntreeSize(int entreeSize) {
    if (entreeSize > MAX_ENTREE_SIZE)
      throw new OSBTreeException("Serialized key-value pair size bigger than allowed " + entreeSize + " vs " + MAX_ENTREE_SIZE
          + ".");
  }

  public void setFreeListPointer(OBonsaiBucketPointer pointer) throws IOException {
    setBucketPointer(offset + FREE_LIST_POINTER_OFFSET, pointer);
  }

  public OBonsaiBucketPointer getFreeListPointer() {
    return getBucketPointer(offset + FREE_LIST_POINTER_OFFSET);
  }

  public void setLeftSibling(OBonsaiBucketPointer pointer) throws IOException {
    setBucketPointer(offset + LEFT_SIBLING_OFFSET, pointer);
  }

  public OBonsaiBucketPointer getLeftSibling() {
    return getBucketPointer(offset + LEFT_SIBLING_OFFSET);
  }

  public void setRightSibling(OBonsaiBucketPointer pointer) throws IOException {
    setBucketPointer(offset + RIGHT_SIBLING_OFFSET, pointer);
  }

  public OBonsaiBucketPointer getRightSibling() {
    return getBucketPointer(offset + RIGHT_SIBLING_OFFSET);
  }

  public static final class SBTreeEntry<K, V> implements Map.Entry<K, V>, Comparable<SBTreeEntry<K, V>> {
    private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

    public final OBonsaiBucketPointer   leftChild;
    public final OBonsaiBucketPointer   rightChild;
    public final K                      key;
    public final V                      value;

    public SBTreeEntry(OBonsaiBucketPointer leftChild, OBonsaiBucketPointer rightChild, K key, V value) {
      this.leftChild = leftChild;
      this.rightChild = rightChild;
      this.key = key;
      this.value = value;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(V value) {
      throw new UnsupportedOperationException("SBTreeEntry.setValue");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      SBTreeEntry that = (SBTreeEntry) o;

      if (!leftChild.equals(that.leftChild))
        return false;
      if (!rightChild.equals(that.rightChild))
        return false;
      if (!key.equals(that.key))
        return false;
      if (value != null ? !value.equals(that.value) : that.value != null)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = leftChild.hashCode();
      result = 31 * result + rightChild.hashCode();
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
