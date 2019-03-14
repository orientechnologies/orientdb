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
import com.orientechnologies.common.util.ORawTriple;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.parser.OInteger;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/7/13
 */
final class OCellBTreeBucketV3<K> extends ODurablePage {
  private static final int RID_SIZE = OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;

  private static final int FREE_POINTER_OFFSET  = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET          = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int IS_LEAF_OFFSET       = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int LEFT_SIBLING_OFFSET  = IS_LEAF_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int RIGHT_SIBLING_OFFSET = LEFT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int FREE_SPACE_OFFSET    = RIGHT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int POSITIONS_ARRAY_OFFSET = FREE_SPACE_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int FREE_SPACE_DEFRAGMENTATION_BOUNDARY = (int) ((MAX_PAGE_SIZE_BYTES - POSITIONS_ARRAY_OFFSET) * 0.3);

  private final boolean isLeaf;

  private final OBinarySerializer<K> keySerializer;

  private final OType[] keyTypes;

  private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

  private final OEncryption encryption;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  OCellBTreeBucketV3(final OCacheEntry cacheEntry, final boolean isLeaf, final OBinarySerializer<K> keySerializer,
      final OType[] keyTypes, final OEncryption encryption) {
    super(cacheEntry);

    this.isLeaf = isLeaf;
    this.keySerializer = keySerializer;
    this.keyTypes = keyTypes;
    this.encryption = encryption;

    setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);
    setIntValue(SIZE_OFFSET, 0);

    setByteValue(IS_LEAF_OFFSET, (byte) (isLeaf ? 1 : 0));
    setLongValue(LEFT_SIBLING_OFFSET, -1);
    setLongValue(RIGHT_SIBLING_OFFSET, -1);

    setIntValue(FREE_SPACE_OFFSET, MAX_PAGE_SIZE_BYTES - POSITIONS_ARRAY_OFFSET);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  OCellBTreeBucketV3(final OCacheEntry cacheEntry, final OBinarySerializer<K> keySerializer, final OType[] keyTypes,
      final OEncryption encryption) {
    super(cacheEntry);
    this.keyTypes = keyTypes;
    this.encryption = encryption;

    this.isLeaf = getByteValue(IS_LEAF_OFFSET) > 0;
    this.keySerializer = keySerializer;
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public int find(final K key) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final K midVal = getKey(mid);
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

  public void remove(final int entryIndex) {
    final int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);
    final int keySize = getIntValue(entryPosition + OIntegerSerializer.INT_SIZE);

    final int entrySize;
    if (isLeaf) {
      entrySize = keySize + RID_SIZE;
    } else {
      throw new IllegalStateException("Remove is applies to leaf buckets only");
    }

    int size = getIntValue(SIZE_OFFSET);
    if (entryIndex < size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE, (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    final int freeSpace = getIntValue(FREE_SPACE_OFFSET);
    size--;

    setIntValue(SIZE_OFFSET, size);
    setIntValue(FREE_SPACE_OFFSET, freeSpace + OIntegerSerializer.INT_SIZE + entrySize);

    setIntValue(entryPosition, -1);
  }

  public int size() {
    return getIntValue(SIZE_OFFSET);
  }

  public SBTreeEntry<K> getEntry(final int entryIndex) {
    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (isLeaf) {
      entryPosition += OIntegerSerializer.INT_SIZE;

      final int keySize = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final K key;
      if (encryption == null) {
        key = deserializeFromDirectMemory(keySerializer, entryPosition);

        entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
      } else {
        final byte[] encryptedKey = getBinaryValue(entryPosition, keySize);
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
    assert !isLeaf;

    final int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    return getIntValue(entryPosition);
  }

  int getRight(final int entryIndex) {
    assert !isLeaf;

    final int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    return getIntValue(entryPosition + OIntegerSerializer.INT_SIZE);
  }

  byte[] getRawEntry(final int entryIndex) {
    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (isLeaf) {
      entryPosition += OIntegerSerializer.INT_SIZE;
      final int keySize = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      return getBinaryValue(entryPosition, keySize + RID_SIZE);
    } else {
      final int startEntryPosition = entryPosition;
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;

      final int keySize;
      if (encryption == null) {
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
   *
   * @return the obtained value.
   */
  public ORID getValue(final int entryIndex) {
    assert isLeaf;

    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    entryPosition += OIntegerSerializer.INT_SIZE;
    // skip key
    entryPosition += getIntValue(entryPosition) + OIntegerSerializer.INT_SIZE;

    final int clusterId = getShortValue(entryPosition);
    final long clusterPosition = getLongValue(entryPosition + OShortSerializer.SHORT_SIZE);

    return new ORecordId(clusterId, clusterPosition);
  }

  byte[] getRawValue(final int entryIndex) {
    assert isLeaf;

    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    //skip key
    entryPosition += OIntegerSerializer.INT_SIZE;
    entryPosition += getIntValue(entryPosition) + OIntegerSerializer.INT_SIZE;

    return getBinaryValue(entryPosition, RID_SIZE);
  }

  public K getKey(final int index) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    entryPosition += OIntegerSerializer.INT_SIZE;

    final int keySize = getIntValue(entryPosition);
    entryPosition += OIntegerSerializer.INT_SIZE;

    if (encryption == null) {
      return deserializeFromDirectMemory(keySerializer, entryPosition);
    } else {
      final byte[] encryptedKey = getBinaryValue(entryPosition, keySize);
      final byte[] serializedKey = encryption.decrypt(encryptedKey);
      return keySerializer.deserializeNativeObject(serializedKey, 0);
    }
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

    if (isLeaf) {
      setIntValue(FREE_SPACE_OFFSET, MAX_PAGE_SIZE_BYTES - POSITIONS_ARRAY_OFFSET);
    }

    setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);

    int index = 0;
    for (final byte[] entry : rawEntries) {
      appendRawEntry(index, entry);
      index++;
    }

    setIntValue(SIZE_OFFSET, newSize);
  }

  boolean addLeafEntry(final int index, final byte[] serializedKey, final byte[] serializedValue) {
    final int entrySize = serializedKey.length + serializedValue.length + 2 * OIntegerSerializer.INT_SIZE;

    assert isLeaf;
    final int size = getIntValue(SIZE_OFFSET);

    final int freeSpace = getIntValue(FREE_SPACE_OFFSET);
    int freePointer = getIntValue(FREE_POINTER_OFFSET);

    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      //do defragmentation only if free space occupies at least N% of page size, otherwise better to split page
      //and as result perform implicit defragmentation
      if (freeSpace >= FREE_SPACE_DEFRAGMENTATION_BOUNDARY) {
        doDefragmentation();

        freePointer = getIntValue(FREE_POINTER_OFFSET);

        if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
          return false;
        }
      } else {
        return false;
      }
    }

    if (index <= size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE, (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    freePointer += setIntValue(freePointer, index);
    freePointer += setIntValue(freePointer, serializedKey.length);

    setBinaryValue(freePointer, serializedKey);
    setBinaryValue(freePointer + serializedKey.length, serializedValue);

    setIntValue(FREE_SPACE_OFFSET, freeSpace - (OIntegerSerializer.INT_SIZE + entrySize));

    return true;
  }

  private void doDefragmentation() {
    //position, size, entryIndex
    final List<ORawTriple<Integer, Integer, Integer>> entries = new ArrayList<>(256);

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);
    int entryPosition = freePointer;
    while (entryPosition < MAX_PAGE_SIZE_BYTES) {
      final int entryIndex = getIntValue(entryPosition);
      final int size = getIntValue(entryPosition + OIntegerSerializer.INT_SIZE) + 2 * OIntegerSerializer.INT_SIZE + RID_SIZE;
      entries.add(new ORawTriple<>(entryPosition, entryIndex, size));

      entryPosition += size;
    }

    int shift = 0;
    int lastDataPosition = 0;
    int mergedDataSize = 0;

    for (int i = entries.size() - 1; i >= 0; i--) {
      final ORawTriple<Integer, Integer, Integer> triple = entries.get(0);

      final int position = triple.first;
      final int size = triple.second;
      final int entryIndex = triple.third;

      if (entryIndex >= 0 && shift > 0) {
        setIntValue(POSITIONS_ARRAY_OFFSET + OIntegerSerializer.INT_SIZE * entryIndex, position + shift);

        lastDataPosition = position;
        mergedDataSize += size; // accumulate consecutive data segments size
      }

      if (mergedDataSize > 0 && (entryIndex < 0 || i == 0)) { // move consecutive merged data segments in one go
        moveData(lastDataPosition, lastDataPosition + shift, mergedDataSize);
        mergedDataSize = 0;
      }

      if (entryIndex < 0) {
        shift += size;
      }
    }

    setIntValue(FREE_POINTER_OFFSET, freePointer + shift);
  }

  private void appendRawEntry(final int index, final byte[] rawEntry) {
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    freePointer -= rawEntry.length;

    if (isLeaf) {
      freePointer -= 2 * OIntegerSerializer.INT_SIZE;
    }

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);

    if (isLeaf) {
      freePointer += setIntValue(freePointer, index);
      freePointer += setIntValue(freePointer, rawEntry.length - RID_SIZE);

      final int freeSpace = getIntValue(FREE_SPACE_OFFSET);
      setIntValue(FREE_SPACE_OFFSET, freeSpace -
          (rawEntry.length + 3 * OIntegerSerializer.INT_SIZE));
    }

    setBinaryValue(freePointer, rawEntry);
  }

  public boolean addEntry(final int index, final SBTreeEntry<K> treeEntry, final boolean updateNeighbors) {
    final byte[] serializedKey = keySerializer.serializeNativeAsWhole(treeEntry.key, (Object[]) keyTypes);
    final int keySize;
    byte[] encryptedKey = null;

    if (encryption == null) {
      keySize = keySerializer.getObjectSize(treeEntry.key, (Object[]) keyTypes);
    } else {
      encryptedKey = encryption.encrypt(serializedKey);
      keySize = encryptedKey.length;
    }

    int entrySize = keySize;

    if (isLeaf) {
      entrySize += RID_SIZE + 2 * OIntegerSerializer.INT_SIZE;
    } else {
      entrySize += 2 * OIntegerSerializer.INT_SIZE;
    }

    int size = size();
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

    if (isLeaf) {
      freePointer += setIntValue(freePointer, index);
      freePointer += setIntValue(freePointer, keySize);

      if (encryption == null) {
        freePointer += setBinaryValue(freePointer, serializedKey);
      } else {
        freePointer += setBinaryValue(freePointer, encryptedKey);
      }

      freePointer += setShortValue(freePointer, (short) treeEntry.value.getClusterId());
      setLongValue(freePointer, treeEntry.value.getClusterPosition());

      final int freeSpace = getIntValue(FREE_SPACE_OFFSET);
      setIntValue(FREE_SPACE_OFFSET, freeSpace - (OIntegerSerializer.INT_SIZE + entrySize));
    } else {
      freePointer += setIntValue(freePointer, treeEntry.leftChild);
      freePointer += setIntValue(freePointer, treeEntry.rightChild);

      if (encryption == null) {
        setBinaryValue(freePointer, serializedKey);
      } else {
        setIntValue(freePointer, encryptedKey.length);
        freePointer += OIntegerSerializer.INT_SIZE;

        setBinaryValue(freePointer, encryptedKey);
      }

      size++;

      if (updateNeighbors && size > 1) {
        if (index < size - 1) {
          final int nextEntryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE);
          setIntValue(nextEntryPosition, treeEntry.rightChild);
        }

        if (index > 0) {
          final int prevEntryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + (index - 1) * OIntegerSerializer.INT_SIZE);
          setIntValue(prevEntryPosition + OIntegerSerializer.INT_SIZE, treeEntry.leftChild);
        }
      }
    }

    return true;
  }

  void updateValue(final int index, final byte[] value) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    entryPosition += OIntegerSerializer.INT_SIZE;

    final int keySize = getIntValue(entryPosition);
    entryPosition += OIntegerSerializer.INT_SIZE + keySize;

    setBinaryValue(entryPosition, value);
  }

  void setLeftSibling(final long pageIndex) {
    setLongValue(LEFT_SIBLING_OFFSET, pageIndex);
  }

  public long getLeftSibling() {
    return getLongValue(LEFT_SIBLING_OFFSET);
  }

  void setRightSibling(final long pageIndex) {
    setLongValue(RIGHT_SIBLING_OFFSET, pageIndex);
  }

  public long getRightSibling() {
    return getLongValue(RIGHT_SIBLING_OFFSET);
  }

  public static final class SBTreeEntry<K> implements Comparable<SBTreeEntry<K>> {
    private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

    final        int  leftChild;
    final        int  rightChild;
    public final K    key;
    public final ORID value;

    SBTreeEntry(final int leftChild, final int rightChild, final K key, final ORID value) {
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
      return leftChild == that.leftChild && rightChild == that.rightChild && Objects.equals(key, that.key) && Objects
          .equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(leftChild, rightChild, key, value);
    }

    @Override
    public String toString() {
      return "SBTreeEntry{" + "leftChild=" + leftChild + ", rightChild=" + rightChild + ", key=" + key + ", value=" + value + '}';
    }

    @Override
    public int compareTo(final SBTreeEntry<K> other) {
      return comparator.compare(key, other.key);
    }
  }
}
