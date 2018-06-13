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

package com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.v1;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OSBTreeBonsaiLocalException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/7/13
 */
public final class OSBTreeBonsaiBucketV1<K, V> extends OBonsaiBucketAbstractV1 {
  static final         int     MAX_BUCKET_SIZE_BYTES    = OGlobalConfiguration.SBTREEBONSAI_BUCKET_SIZE.getValueAsInteger() * 1024;
  /**
   * Maximum size of key-value pair which can be put in SBTreeBonsai in bytes (24576000 by default)
   */
  private static final byte    LEAF                     = 0x1;
  private static final byte    DELETED                  = 0x2;
  private static final int     MAX_ENTREE_SIZE          = 24576000;
  private static final int     FREE_POINTER_OFFSET      = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int     SIZE_OFFSET              = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int     FLAGS_OFFSET             = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int     FREE_LIST_POINTER_OFFSET = FLAGS_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int     LEFT_SIBLING_OFFSET      = FREE_LIST_POINTER_OFFSET + OBonsaiBucketPointer.SIZE;
  private static final int     RIGHT_SIBLING_OFFSET     = LEFT_SIBLING_OFFSET + OBonsaiBucketPointer.SIZE;
  private static final int     TREE_SIZE_OFFSET         = RIGHT_SIBLING_OFFSET + OBonsaiBucketPointer.SIZE;
  private static final int     KEY_SERIALIZER_OFFSET    = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int     VALUE_SERIALIZER_OFFSET  = KEY_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int     POSITIONS_ARRAY_OFFSET   = VALUE_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;
  private final        boolean isLeaf;
  private final        int     offset;

  private final OBinarySerializer<K> keySerializer;
  private final OBinarySerializer<V> valueSerializer;

  private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

  private final OSBTreeBonsaiLocalV1<K, V> tree;

  public static final class SBTreeEntry<K, V> implements Map.Entry<K, V>, Comparable<SBTreeEntry<K, V>> {
    final         OBonsaiBucketPointer  leftChild;
    final         OBonsaiBucketPointer  rightChild;
    public final  K                     key;
    public final  V                     value;
    private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

    SBTreeEntry(OBonsaiBucketPointer leftChild, OBonsaiBucketPointer rightChild, K key, V value) {
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

  OSBTreeBonsaiBucketV1(OCacheEntry cacheEntry, int pageOffset, boolean isLeaf, OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer, OSBTreeBonsaiLocalV1<K, V> tree) {
    super(cacheEntry);

    this.offset = pageOffset;
    this.isLeaf = isLeaf;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;

    buffer.putInt(offset + FREE_POINTER_OFFSET, MAX_BUCKET_SIZE_BYTES);
    buffer.putInt(offset + SIZE_OFFSET, 0);

    //THIS REMOVE ALSO THE EVENTUAL DELETED FLAG
    buffer.put(offset + FLAGS_OFFSET, isLeaf ? LEAF : 0);
    buffer.putLong(offset + LEFT_SIBLING_OFFSET, -1);
    buffer.putLong(offset + RIGHT_SIBLING_OFFSET, -1);

    buffer.putLong(offset + TREE_SIZE_OFFSET, 0);

    buffer.put(offset + KEY_SERIALIZER_OFFSET, keySerializer.getId());
    buffer.put(offset + VALUE_SERIALIZER_OFFSET, valueSerializer.getId());

    cacheEntry.markDirty();

    this.tree = tree;
  }

  OSBTreeBonsaiBucketV1(OCacheEntry cacheEntry, int pageOffset, OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer, OSBTreeBonsaiLocalV1<K, V> tree) {
    super(cacheEntry);

    this.offset = pageOffset;
    this.isLeaf = (getByteValue(offset + FLAGS_OFFSET) & LEAF) == LEAF;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.tree = tree;
  }

  byte getKeySerializerId() {
    return buffer.get(offset + KEY_SERIALIZER_OFFSET);
  }

  byte getValueSerializerId() {
    return buffer.get(offset + VALUE_SERIALIZER_OFFSET);
  }

  long getTreeSize() {
    return buffer.getLong(offset + TREE_SIZE_OFFSET);
  }

  void setTreeSize(long size) {
    buffer.putLong(offset + TREE_SIZE_OFFSET, size);
    cacheEntry.markDirty();
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

  public void remove(int entryIndex) {
    int entryPosition = buffer.getInt(offset + POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);

    buffer.position(offset + entryPosition);
    int entrySize = keySerializer.getObjectSizeInByteBuffer(buffer);
    if (isLeaf) {
      assert valueSerializer.isFixedLength();
      entrySize += valueSerializer.getFixedLength();
    } else {
      throw new IllegalStateException("Remove is applies to leaf buckets only");
    }

    cacheEntry.markDirty();
    int size = size();
    if (entryIndex < size - 1) {
      moveData(offset + POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          offset + POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE,
          (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE, buffer);
    }

    size--;
    buffer.putInt(offset + SIZE_OFFSET, size);

    int freePointer = buffer.getInt(offset + FREE_POINTER_OFFSET);
    if (size > 0 && entryPosition > freePointer) {
      moveData(offset + freePointer, offset + freePointer + entrySize, entryPosition - freePointer, buffer);
    }
    buffer.putInt(offset + FREE_POINTER_OFFSET, freePointer + entrySize);

    int currentPositionOffset = offset + POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      int currentEntryPosition = buffer.getInt(currentPositionOffset);
      if (currentEntryPosition < entryPosition) {
        buffer.putInt(currentPositionOffset, currentEntryPosition + entrySize);
      }
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }
  }

  public int size() {
    return buffer.getInt(offset + SIZE_OFFSET);
  }

  public SBTreeEntry<K, V> getEntry(int entryIndex) {
    final ByteBuffer buffer = getBufferDuplicate();

    int entryPosition = buffer.getInt(offset + entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (isLeaf) {
      buffer.position(offset + entryPosition);
      K key = keySerializer.deserializeFromByteBufferObject(buffer);

      buffer.position(offset + entryPosition);
      entryPosition += keySerializer.getObjectSizeInByteBuffer(buffer);

      buffer.position(offset + entryPosition);
      V value = valueSerializer.deserializeFromByteBufferObject(buffer);

      return new SBTreeEntry<>(OBonsaiBucketPointer.NULL, OBonsaiBucketPointer.NULL, key, value);
    } else {
      OBonsaiBucketPointer leftChild = getBucketPointer(offset + entryPosition);
      entryPosition += OBonsaiBucketPointer.SIZE;

      OBonsaiBucketPointer rightChild = getBucketPointer(offset + entryPosition);
      entryPosition += OBonsaiBucketPointer.SIZE;

      buffer.position(offset + entryPosition);
      K key = keySerializer.deserializeFromByteBufferObject(buffer);

      return new SBTreeEntry<>(leftChild, rightChild, key, null);
    }
  }

  byte[] getRawEntry(int entryIndex) {
    final ByteBuffer buffer = getBufferDuplicate();

    int entryPosition = buffer.getInt(offset + entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    final int startEntryPosition = entryPosition;

    if (isLeaf) {
      buffer.position(offset + entryPosition);
      final int keySize = keySerializer.getObjectSizeInByteBuffer(buffer);
      entryPosition += keySize;

      buffer.position(offset + entryPosition);
      final int valueSize = valueSerializer.getObjectSizeInByteBuffer(buffer);

      buffer.position(startEntryPosition + offset);
      final byte[] value = new byte[keySize + valueSize];
      buffer.get(value);

      return value;
    } else {
      entryPosition += 2 * OBonsaiBucketPointer.SIZE;

      buffer.position(offset + entryPosition);
      final int keyLen = keySerializer.getObjectSizeInByteBuffer(buffer);

      final byte[] value = new byte[keyLen + 2 * OBonsaiBucketPointer.SIZE];
      buffer.position(startEntryPosition + offset);
      buffer.get(value);

      return value;
    }
  }

  byte[][] getRawLeafEntry(int entryIndex) {
    final ByteBuffer buffer = getBufferDuplicate();

    int entryPosition = buffer.getInt(offset + entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    assert isLeaf;

    buffer.position(offset + entryPosition);
    final int keyLen = keySerializer.getObjectSizeInByteBuffer(buffer);

    buffer.position(offset + entryPosition);
    final byte[] rawKey = new byte[keyLen];
    buffer.get(rawKey);

    entryPosition += rawKey.length;

    buffer.position(offset + entryPosition);
    final int valueLen = valueSerializer.getObjectSizeInByteBuffer(buffer);
    final byte[] rawValue = new byte[valueLen];

    buffer.position(offset + entryPosition);
    buffer.get(rawValue);

    return new byte[][] { rawKey, rawValue };
  }

  byte[] getRawValue(int entryIndex, int keyLen) {
    final ByteBuffer buffer = getBufferDuplicate();

    int entryPosition = buffer.getInt(offset + entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    assert isLeaf;

    entryPosition += keyLen;

    buffer.position(offset + entryPosition);
    final int valueLen = valueSerializer.getObjectSizeInByteBuffer(buffer);

    final byte[] rawValue = new byte[valueLen];
    buffer.position(offset + entryPosition);
    buffer.get(rawValue);

    return rawValue;
  }

  public K getKey(int index) {
    final ByteBuffer buffer = getBufferDuplicate();
    int entryPosition = buffer.getInt(offset + index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf) {
      entryPosition += 2 * (OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE);
    }

    buffer.position(offset + entryPosition);
    return keySerializer.deserializeFromByteBufferObject(buffer);
  }

  boolean isLeaf() {
    return isLeaf;
  }

  public void addAll(List<byte[]> entries) {
    cacheEntry.markDirty();

    for (int i = 0; i < entries.size(); i++) {
      appendRawEntry(i, entries.get(i));
    }

    buffer.putInt(offset + SIZE_OFFSET, entries.size());
  }

  public void shrink(int newSize) {
    cacheEntry.markDirty();

    List<byte[]> rawEntries = new ArrayList<>(newSize);

    for (int i = 0; i < newSize; i++) {
      rawEntries.add(getRawEntry(i));
    }

    buffer.putInt(offset + FREE_POINTER_OFFSET, MAX_BUCKET_SIZE_BYTES);

    int index = 0;
    for (byte[] entry : rawEntries) {
      appendRawEntry(index, entry);
      index++;
    }

    buffer.putInt(offset + SIZE_OFFSET, newSize);
  }

  public boolean addEntry(int index, SBTreeEntry<K, V> treeEntry, boolean updateNeighbors) {
    final int keySize = keySerializer.getObjectSize(treeEntry.key);
    int valueSize = 0;
    int entrySize = keySize;

    if (isLeaf) {
      assert valueSerializer.isFixedLength();
      valueSize = valueSerializer.getFixedLength();

      entrySize += valueSize;

      checkEntreeSize(entrySize);
    } else
      entrySize += 2 * (OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE);

    int size = size();
    int freePointer = buffer.getInt(offset + FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      if (size > 1) {
        return false;
      } else
        throw new OSBTreeBonsaiLocalException(
            "Entry size ('key + value') is more than is more than allowed " + (freePointer - 2 * OIntegerSerializer.INT_SIZE
                + POSITIONS_ARRAY_OFFSET) + " bytes, either increase page size using '"
                + OGlobalConfiguration.SBTREEBONSAI_BUCKET_SIZE.getKey() + "' parameter, or decrease 'key + value' size.", tree);
    }

    cacheEntry.markDirty();

    if (index <= size - 1) {
      moveData(offset + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          offset + POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE, (size - index) * OIntegerSerializer.INT_SIZE,
          buffer);
    }

    freePointer -= entrySize;

    buffer.putInt(offset + FREE_POINTER_OFFSET, freePointer);
    buffer.putInt(offset + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    buffer.putInt(offset + SIZE_OFFSET, size + 1);

    if (isLeaf) {
      byte[] serializedKey = new byte[keySize];
      keySerializer.serializeNativeObject(treeEntry.key, serializedKey, 0);

      buffer.position(offset + freePointer);
      buffer.put(serializedKey);

      byte[] serializedValue = new byte[valueSize];
      valueSerializer.serializeNativeObject(treeEntry.value, serializedValue, 0);

      buffer.put(serializedValue);
    } else {
      setBucketPointer(offset + freePointer, treeEntry.leftChild);
      freePointer += OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;

      setBucketPointer(offset + freePointer, treeEntry.rightChild);
      freePointer += OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;

      byte[] serializedKey = new byte[keySize];
      keySerializer.serializeNativeObject(treeEntry.key, serializedKey, 0);

      buffer.position(offset + freePointer);
      buffer.put(serializedKey);

      size++;

      if (updateNeighbors && size > 1) {
        if (index < size - 1) {
          final int nextEntryPosition = buffer.getInt(offset + POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE);
          setBucketPointer(offset + nextEntryPosition, treeEntry.rightChild);
        }

        if (index > 0) {
          final int prevEntryPosition = buffer.getInt(offset + POSITIONS_ARRAY_OFFSET + (index - 1) * OIntegerSerializer.INT_SIZE);
          setBucketPointer(offset + prevEntryPosition + OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE,
              treeEntry.leftChild);
        }
      }
    }

    return true;
  }

  private void appendRawEntry(int index, byte[] rawEntry) {
    int freePointer = buffer.getInt(offset + FREE_POINTER_OFFSET);

    freePointer -= rawEntry.length;

    buffer.putInt(offset + FREE_POINTER_OFFSET, freePointer);
    buffer.putInt(offset + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);

    buffer.position(offset + freePointer);
    buffer.put(rawEntry);
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  boolean addLeafEntry(int index, byte[] serializedKey, byte[] serializedValue) {
    final int keySize = serializedKey.length;
    final int valueSize = serializedValue.length;

    int entrySize = keySize;

    assert isLeaf;
    assert valueSerializer.isFixedLength();

    entrySize += valueSize;

    checkEntreeSize(entrySize);

    int size = size();
    int freePointer = buffer.getInt(offset + FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      if (size > 1) {
        return false;
      } else {
        throw new OSBTreeBonsaiLocalException(
            "Entry size ('key + value') is more than is more than allowed " + (freePointer - 2 * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) + " bytes, either increase page size using '"
                + OGlobalConfiguration.SBTREEBONSAI_BUCKET_SIZE.getKey() + "' parameter, or decrease 'key + value' size.", tree);
      }
    }

    cacheEntry.markDirty();

    if (index <= size - 1) {
      moveData(offset + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          offset + POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE, (size - index) * OIntegerSerializer.INT_SIZE,
          buffer);
    }

    freePointer -= entrySize;

    buffer.putInt(offset + FREE_POINTER_OFFSET, freePointer);
    buffer.putInt(offset + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    buffer.putInt(offset + SIZE_OFFSET, size + 1);

    buffer.position(offset + freePointer);
    buffer.put(serializedKey);
    buffer.put(serializedValue);

    return true;
  }

  void updateRawValue(int index, int keySize, byte[] rawValue) {
    int entryPosition = buffer.getInt(offset + index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    entryPosition += keySize;

    buffer.position(offset + entryPosition);
    buffer.put(rawValue);

    cacheEntry.markDirty();
  }

  void updateValue(int index, V value) {
    assert valueSerializer.isFixedLength();

    int entryPosition = buffer.getInt(offset + index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    buffer.position(offset + entryPosition);
    final int keySize = keySerializer.getObjectSizeInByteBuffer(buffer);
    entryPosition += keySize;

    final int size = valueSerializer.getFixedLength();

    byte[] serializedValue = new byte[size];
    valueSerializer.serializeNativeObject(value, serializedValue, 0);

    buffer.position(offset + entryPosition);
    byte[] oldSerializedValue = new byte[size];
    buffer.get(oldSerializedValue);

    if (ODefaultComparator.INSTANCE.compare(oldSerializedValue, serializedValue) == 0) {
      return;
    }

    buffer.position(offset + entryPosition);
    buffer.put(serializedValue);

    cacheEntry.markDirty();
  }

  OBonsaiBucketPointer getFreeListPointer() {
    return getBucketPointer(offset + FREE_LIST_POINTER_OFFSET);
  }

  void setFreeListPointer(OBonsaiBucketPointer pointer) {
    setBucketPointer(offset + FREE_LIST_POINTER_OFFSET, pointer);
  }

  void setDeleted() {
    byte value = buffer.get(offset + FLAGS_OFFSET);
    buffer.put(offset + FLAGS_OFFSET, (byte) (value | DELETED));

    cacheEntry.markDirty();
  }

  public boolean isDeleted() {
    return (buffer.get(offset + FLAGS_OFFSET) & DELETED) == DELETED;
  }

  OBonsaiBucketPointer getLeftSibling() {
    return getBucketPointer(offset + LEFT_SIBLING_OFFSET);
  }

  void setLeftSibling(OBonsaiBucketPointer pointer) {
    setBucketPointer(offset + LEFT_SIBLING_OFFSET, pointer);
  }

  OBonsaiBucketPointer getRightSibling() {
    return getBucketPointer(offset + RIGHT_SIBLING_OFFSET);
  }

  void setRightSibling(OBonsaiBucketPointer pointer) {
    setBucketPointer(offset + RIGHT_SIBLING_OFFSET, pointer);
  }

  private void checkEntreeSize(int entreeSize) {
    if (entreeSize > MAX_ENTREE_SIZE)
      throw new OSBTreeBonsaiLocalException(
          "Serialized key-value pair size bigger than allowed " + entreeSize + " vs " + MAX_ENTREE_SIZE + ".", tree);
  }
}
