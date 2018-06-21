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

package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.PageSerializationType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/7/13
 */
public final class OSBTreeBucket<K, V> extends ODurablePage {
  private static final int FREE_POINTER_OFFSET  = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET          = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int IS_LEAF_OFFSET       = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int LEFT_SIBLING_OFFSET  = IS_LEAF_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int RIGHT_SIBLING_OFFSET = LEFT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int TREE_SIZE_OFFSET = RIGHT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  /**
   * KEY_SERIALIZER_OFFSET and VALUE_SERIALIZER_OFFSET are no longer used by sb-tree since 1.7.
   * However we left them in buckets to support backward compatibility.
   */
  private static final int KEY_SERIALIZER_OFFSET   = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int VALUE_SERIALIZER_OFFSET = KEY_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;

  private static final int FREE_VALUES_LIST_OFFSET = VALUE_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;

  private static final int POSITIONS_ARRAY_OFFSET = FREE_VALUES_LIST_OFFSET + OLongSerializer.LONG_SIZE;

  private boolean isLeaf;

  private final OBinarySerializer<K> keySerializer;
  private final OBinarySerializer<V> valueSerializer;

  private final OType[] keyTypes;

  private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

  private final OEncryption encryption;

  public OSBTreeBucket(final OCacheEntry cacheEntry) {
    super(cacheEntry);

    keySerializer = null;
    valueSerializer = null;
    keyTypes = null;
    encryption = null;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  OSBTreeBucket(final OCacheEntry cacheEntry, final boolean isLeaf, final OBinarySerializer<K> keySerializer,
      final OType[] keyTypes, final OBinarySerializer<V> valueSerializer, final OEncryption encryption) {
    super(cacheEntry);

    this.isLeaf = isLeaf;
    this.keySerializer = keySerializer;
    this.keyTypes = keyTypes;
    this.valueSerializer = valueSerializer;
    this.encryption = encryption;

    buffer.putInt(FREE_POINTER_OFFSET, PAGE_SIZE);
    buffer.putInt(SIZE_OFFSET, 0);

    buffer.put(IS_LEAF_OFFSET, (byte) (isLeaf ? 1 : 0));
    buffer.putLong(LEFT_SIBLING_OFFSET, -1);
    buffer.putLong(RIGHT_SIBLING_OFFSET, -1);

    buffer.putLong(TREE_SIZE_OFFSET, 0);
    buffer.putLong(FREE_VALUES_LIST_OFFSET, -1);

    buffer.put(KEY_SERIALIZER_OFFSET, this.keySerializer.getId());
    buffer.put(VALUE_SERIALIZER_OFFSET, this.valueSerializer.getId());

    cacheEntry.markDirty();
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  OSBTreeBucket(final OCacheEntry cacheEntry, final OBinarySerializer<K> keySerializer, final OType[] keyTypes,
      final OBinarySerializer<V> valueSerializer, final OEncryption encryption) {
    super(cacheEntry);

    this.keyTypes = keyTypes;
    this.encryption = encryption;

    this.isLeaf = buffer.get(IS_LEAF_OFFSET) > 0;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  void setTreeSize(final long size) {
    buffer.putLong(TREE_SIZE_OFFSET, size);
    cacheEntry.markDirty();
  }

  long getTreeSize() {
    return buffer.getLong(TREE_SIZE_OFFSET);
  }

  public boolean isEmpty() {
    return buffer.getInt(SIZE_OFFSET) == 0;
  }

  long getValuesFreeListFirstIndex() {
    return buffer.getLong(FREE_VALUES_LIST_OFFSET);
  }

  void setValuesFreeListFirstIndex(final long pageIndex) {
    buffer.putLong(FREE_VALUES_LIST_OFFSET, pageIndex);
    cacheEntry.markDirty();
  }

  public int find(final K key) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final K midVal = getKey(mid);
      final int cmp = comparator.compare(midVal, key);

      if (cmp < 0)
        low = mid + 1;
      else if (cmp > 0)
        high = mid - 1;
      else
        return mid; // key found
    }
    return -(low + 1); // key not found.
  }

  public void remove(final int entryIndex) {
    final int entryPosition = buffer.getInt(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);
    final int keySize;

    if (encryption == null) {
      buffer.position(entryPosition);
      keySize = keySerializer.getObjectSizeInByteBuffer(buffer);
    } else {
      final int encryptedSize = buffer.getInt(entryPosition);
      keySize = OIntegerSerializer.INT_SIZE + encryptedSize;
    }

    final int entrySize;
    if (isLeaf) {
      if (valueSerializer.isFixedLength()) {
        entrySize = keySize + valueSerializer.getFixedLength() + OByteSerializer.BYTE_SIZE;
      } else {
        assert buffer.get(entryPosition + keySize) == 0;
        buffer.position(entryPosition + keySize + OByteSerializer.BYTE_SIZE);
        entrySize = keySize + valueSerializer.getObjectSizeInByteBuffer(buffer) + OByteSerializer.BYTE_SIZE;
      }
    } else {
      throw new IllegalStateException("Remove is applies to leaf buckets only");
    }

    cacheEntry.markDirty();

    int size = buffer.getInt(SIZE_OFFSET);
    if (entryIndex < size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE, (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    buffer.putInt(SIZE_OFFSET, size);

    final int freePointer = buffer.getInt(FREE_POINTER_OFFSET);
    if (size > 0 && entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    buffer.putInt(FREE_POINTER_OFFSET, freePointer + entrySize);

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      final int currentEntryPosition = buffer.getInt(currentPositionOffset);
      if (currentEntryPosition < entryPosition) {
        buffer.putInt(currentPositionOffset, currentEntryPosition + entrySize);
      }
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }
  }

  public int size() {
    return buffer.getInt(SIZE_OFFSET);
  }

  public SBTreeEntry<K, V> getEntry(final int entryIndex) {
    int entryPosition = buffer.getInt(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    final ByteBuffer buffer = getBufferDuplicate();
    if (isLeaf) {
      final K key;
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

        entryPosition += encryptedKey.length;

        key = keySerializer.deserializeNativeObject(encryption.decrypt(encryptedKey), 0);
      }

      final boolean isLinkValue = buffer.get(entryPosition) > 0;
      long link = -1;
      V value = null;

      if (isLinkValue) {
        link = buffer.getLong(entryPosition + OByteSerializer.BYTE_SIZE);
      } else {
        buffer.position(entryPosition + OByteSerializer.BYTE_SIZE);
        value = valueSerializer.deserializeFromByteBufferObject(buffer);
      }

      return new SBTreeEntry<>(-1, -1, key, new OSBTreeValue<>(link >= 0, link, value));
    } else {
      buffer.position(entryPosition);
      final long leftChild = buffer.getLong();
      final long rightChild = buffer.getLong();

      final K key;

      if (encryption == null) {
        key = keySerializer.deserializeFromByteBufferObject(buffer);
      } else {
        final int encryptedSize = buffer.getInt();

        final byte[] encryptedKey = new byte[encryptedSize];
        buffer.get(encryptedKey);

        key = keySerializer.deserializeNativeObject(encryption.decrypt(encryptedKey), 0);
      }

      return new SBTreeEntry<>(leftChild, rightChild, key, null);
    }
  }

  byte[] getRawEntry(final int entryIndex) {
    int entryPosition = buffer.getInt(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    final int startEntryPosition = entryPosition;

    final ByteBuffer buffer = getBufferDuplicate();

    if (isLeaf) {
      final int keySize;
      if (encryption == null) {
        buffer.position(entryPosition);
        keySize = keySerializer.getObjectSizeInByteBuffer(buffer);
      } else {
        final int encryptedSize = buffer.getInt(entryPosition);
        keySize = OIntegerSerializer.INT_SIZE + encryptedSize;
      }

      entryPosition += keySize;

      assert buffer.get(entryPosition) == 0;

      buffer.position(entryPosition + OByteSerializer.BYTE_SIZE);
      final int valueSize = valueSerializer.getObjectSizeInByteBuffer(buffer);

      final byte[] entry = new byte[keySize + valueSize + OByteSerializer.BYTE_SIZE];
      buffer.position(startEntryPosition);
      buffer.get(entry);

      return entry;
    } else {
      entryPosition += 2 * OLongSerializer.LONG_SIZE;

      final int keySize;
      if (encryption == null) {
        buffer.position(entryPosition);
        keySize = keySerializer.getObjectSizeInByteBuffer(buffer);
      } else {
        final int encryptedSize = buffer.getInt(entryPosition);
        keySize = OIntegerSerializer.INT_SIZE + encryptedSize;
      }

      final byte[] entry = new byte[keySize + 2 * OLongSerializer.LONG_SIZE];
      buffer.position(startEntryPosition);
      buffer.get(entry);

      return entry;
    }
  }

  /**
   * Obtains the value stored under the given entry index in this bucket.
   *
   * @param entryIndex the value entry index.
   *
   * @return the obtained value.
   */
  public OSBTreeValue<V> getValue(final int entryIndex) {
    assert isLeaf;

    int entryPosition = buffer.getInt(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    final ByteBuffer buffer = getBufferDuplicate();
    // skip key
    if (encryption == null) {
      buffer.position(entryPosition);
      entryPosition += keySerializer.getObjectSizeInByteBuffer(buffer);
    } else {
      final int encryptedSize = buffer.getInt(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE + encryptedSize;
    }

    final boolean isLinkValue = buffer.get(entryPosition) > 0;
    long link = -1;
    V value = null;

    if (isLinkValue) {
      link = buffer.getLong(entryPosition + OByteSerializer.BYTE_SIZE);
    } else {
      buffer.position(entryPosition + OByteSerializer.BYTE_SIZE);
      value = valueSerializer.deserializeFromByteBufferObject(buffer);
    }

    return new OSBTreeValue<>(link >= 0, link, value);
  }

  byte[] getRawValue(final int entryIndex) {
    assert isLeaf;

    int entryPosition = buffer.getInt(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    final ByteBuffer buffer = getBufferDuplicate();

    // skip key
    if (encryption == null) {
      buffer.position(entryPosition);
      entryPosition += keySerializer.getObjectSizeInByteBuffer(buffer);
    } else {
      final int encryptedSize = buffer.getInt(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE + encryptedSize;
    }

    assert buffer.get(entryPosition) == 0;

    buffer.position(entryPosition + OByteSerializer.BYTE_SIZE);
    final int valueSize = valueSerializer.getObjectSizeInByteBuffer(buffer);

    final byte[] rawValue = new byte[valueSize];
    buffer.position(entryPosition + OByteSerializer.BYTE_SIZE);
    buffer.get(rawValue);

    return rawValue;
  }

  public K getKey(final int index) {
    int entryPosition = buffer.getInt(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf) {
      entryPosition += 2 * OLongSerializer.LONG_SIZE;
    }

    final ByteBuffer buffer = getBufferDuplicate();

    if (encryption == null) {
      buffer.position(entryPosition);
      return keySerializer.deserializeFromByteBufferObject(buffer);
    }

    final int encryptedSize = buffer.getInt(entryPosition);
    entryPosition += OIntegerSerializer.INT_SIZE;

    final byte[] encryptedKey = new byte[encryptedSize];
    buffer.position(entryPosition);
    buffer.get(encryptedKey);

    return keySerializer.deserializeNativeObject(encryption.decrypt(encryptedKey), 0);
  }

  boolean isLeaf() {
    return isLeaf;
  }

  public void addAll(final List<byte[]> rawEntries) {
    cacheEntry.markDirty();

    for (int i = 0; i < rawEntries.size(); i++) {
      appendRawEntry(i, rawEntries.get(i), buffer);
    }

    buffer.putInt(SIZE_OFFSET, rawEntries.size());
  }

  public void shrink(final int newSize) {
    cacheEntry.markDirty();

    final List<byte[]> rawEntries = new ArrayList<>(newSize);

    for (int i = 0; i < newSize; i++) {
      rawEntries.add(getRawEntry(i));
    }

    buffer.putInt(FREE_POINTER_OFFSET, PAGE_SIZE);

    int index = 0;
    for (final byte[] entry : rawEntries) {
      appendRawEntry(index, entry, buffer);
      index++;
    }

    buffer.putInt(SIZE_OFFSET, newSize);
  }

  @Override
  public int serializedSize() {
    final int bucketSize = this.buffer.getInt(SIZE_OFFSET);
    final int positionsEndPointer = POSITIONS_ARRAY_OFFSET + bucketSize * OIntegerSerializer.INT_SIZE;
    int size = positionsEndPointer;

    final int freePointer = this.buffer.getInt(FREE_POINTER_OFFSET);
    final int entriesSize = PAGE_SIZE - freePointer;
    size += entriesSize;

    return size;
  }

  @Override
  public void serializePage(ByteBuffer recordBuffer) {
    assert buffer.limit() == buffer.capacity();

    final int bucketSize = this.buffer.getInt(SIZE_OFFSET);
    final int positionsEndPointer = POSITIONS_ARRAY_OFFSET + bucketSize * OIntegerSerializer.INT_SIZE;

    final int freePointer = this.buffer.getInt(FREE_POINTER_OFFSET);
    final int entriesSize = PAGE_SIZE - freePointer;

    this.buffer.position(0);
    this.buffer.limit(positionsEndPointer);
    recordBuffer.put(this.buffer);
    this.buffer.limit(this.buffer.capacity());

    if (entriesSize > 0) {
      this.buffer.position(freePointer);
      this.buffer.limit(freePointer + entriesSize);
      recordBuffer.put(this.buffer);
      this.buffer.limit(this.buffer.capacity());
    }
  }

  @Override
  public void deserializePage(final byte[] page) {
    assert buffer.limit() == buffer.capacity();

    buffer.position(0);
    buffer.put(page, 0, POSITIONS_ARRAY_OFFSET);

    final int bucketSize = buffer.getInt(SIZE_OFFSET);
    final int positionsSize = bucketSize * OIntegerSerializer.INT_SIZE;

    if (bucketSize > 0) {
      buffer.put(page, POSITIONS_ARRAY_OFFSET, positionsSize);
    }

    final int freePointer = buffer.getInt(FREE_POINTER_OFFSET);
    final int entriesSize = PAGE_SIZE - freePointer;

    if (entriesSize > 0) {
      buffer.position(freePointer);
      buffer.put(page, POSITIONS_ARRAY_OFFSET + positionsSize, entriesSize);
    }

    this.isLeaf = buffer.get(IS_LEAF_OFFSET) > 0;

    cacheEntry.markDirty();
  }

  @Override
  protected PageSerializationType serializationType() {
    return PageSerializationType.SBTREE_BUCKET;
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  boolean addLeafEntry(final int index, final byte[] serializedKey, final byte[] serializedValue) {
    final int entrySize = serializedKey.length + serializedValue.length + OByteSerializer.BYTE_SIZE;

    assert isLeaf;
    final int size = buffer.getInt(SIZE_OFFSET);

    int freePointer = buffer.getInt(FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      return false;
    }

    cacheEntry.markDirty();

    if (index <= size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE, (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    buffer.putInt(FREE_POINTER_OFFSET, freePointer);
    buffer.putInt(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    buffer.putInt(SIZE_OFFSET, size + 1);

    buffer.position(freePointer);

    buffer.put(serializedKey);
    buffer.put((byte) 0);
    buffer.put(serializedValue);

    return true;
  }

  private static void appendRawEntry(final int index, final byte[] rawEntry, final ByteBuffer buffer) {
    int freePointer = buffer.getInt(FREE_POINTER_OFFSET);
    freePointer -= rawEntry.length;

    buffer.putInt(FREE_POINTER_OFFSET, freePointer);
    buffer.putInt(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);

    buffer.position(freePointer);
    buffer.put(rawEntry);
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  public boolean addEntry(final int index, final SBTreeEntry<K, V> treeEntry, final boolean updateNeighbors) {
    final int keySize;
    byte[] encryptedKey = null;

    if (encryption == null) {
      keySize = keySerializer.getObjectSize(treeEntry.key, (Object[]) keyTypes);
    } else {
      final int serializedSize = keySerializer.getObjectSize(treeEntry.key, (Object[]) keyTypes);
      final byte[] serializedKey = new byte[serializedSize];

      keySerializer.serializeNativeObject(treeEntry.key, serializedKey, 0, (Object[]) keyTypes);
      encryptedKey = encryption.encrypt(serializedKey);
      keySize = OIntegerSerializer.INT_SIZE + encryptedKey.length;
    }

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
    int freePointer = buffer.getInt(FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      return false;
    }

    cacheEntry.markDirty();
    if (index <= size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE, (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    buffer.putInt(FREE_POINTER_OFFSET, freePointer);
    buffer.putInt(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    buffer.putInt(SIZE_OFFSET, size + 1);

    buffer.position(freePointer);
    if (isLeaf) {
      if (encryption == null) {
        final byte[] serializedKey = new byte[keySize];
        keySerializer.serializeNativeObject(treeEntry.key, serializedKey, 0, (Object[]) keyTypes);

        buffer.put(serializedKey);
      } else {
        buffer.putInt(encryptedKey.length);
        buffer.put(encryptedKey);
      }

      buffer.put(treeEntry.value.isLink() ? (byte) 1 : (byte) 0);

      final byte[] serializedValue = new byte[valueSize];
      if (treeEntry.value.isLink())
        OLongSerializer.INSTANCE.serializeNative(treeEntry.value.getLink(), serializedValue, 0);
      else
        valueSerializer.serializeNativeObject(treeEntry.value.getValue(), serializedValue, 0);

      buffer.put(serializedValue);
    } else {
      buffer.putLong(treeEntry.leftChild);
      buffer.putLong(treeEntry.rightChild);

      if (encryption == null) {
        final byte[] serializedKey = new byte[keySize];
        keySerializer.serializeNativeObject(treeEntry.key, serializedKey, 0, (Object[]) keyTypes);

        buffer.put(serializedKey);
      } else {
        buffer.putInt(encryptedKey.length);
        buffer.put(encryptedKey);
      }

      size++;

      if (updateNeighbors && size > 1) {
        if (index < size - 1) {
          final int nextEntryPosition = buffer.getInt(POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE);
          buffer.putLong(nextEntryPosition, treeEntry.rightChild);
        }

        if (index > 0) {
          final int prevEntryPosition = buffer.getInt(POSITIONS_ARRAY_OFFSET + (index - 1) * OIntegerSerializer.INT_SIZE);
          buffer.putLong(prevEntryPosition + OLongSerializer.LONG_SIZE, treeEntry.leftChild);
        }
      }
    }

    return true;
  }

  void updateValue(final int index, final byte[] value) {
    cacheEntry.markDirty();

    int entryPosition = buffer.getInt(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (encryption == null) {
      buffer.position(entryPosition);
      entryPosition += keySerializer.getObjectSizeInByteBuffer(buffer);
    } else {
      final int encryptedValue = buffer.getInt(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE + encryptedValue;
    }

    assert buffer.get(entryPosition) == 0;

    entryPosition += OByteSerializer.BYTE_SIZE;

    buffer.position(entryPosition);
    buffer.put(value);
  }

  void setLeftSibling(final long pageIndex) {
    buffer.putLong(LEFT_SIBLING_OFFSET, pageIndex);
    cacheEntry.markDirty();
  }

  long getLeftSibling() {
    return buffer.getLong(LEFT_SIBLING_OFFSET);
  }

  void setRightSibling(final long pageIndex) {
    buffer.putLong(RIGHT_SIBLING_OFFSET, pageIndex);
    cacheEntry.markDirty();
  }

  long getRightSibling() {
    return buffer.getLong(RIGHT_SIBLING_OFFSET);
  }

  public static final class SBTreeEntry<K, V> implements Comparable<SBTreeEntry<K, V>> {
    private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

    final        long            leftChild;
    final        long            rightChild;
    public final K               key;
    public final OSBTreeValue<V> value;

    SBTreeEntry(final long leftChild, final long rightChild, final K key, final OSBTreeValue<V> value) {
      this.leftChild = leftChild;
      this.rightChild = rightChild;
      this.key = key;
      this.value = value;
    }

    @Override
    public boolean equals(final Object o) {
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
        if (!value.equals(that.value))
          return false;
      } else {
        if (that.value != null)
          return false;
      }

      return true;
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
    public int compareTo(final SBTreeEntry<K, V> other) {
      return comparator.compare(key, other.key);
    }
  }
}
