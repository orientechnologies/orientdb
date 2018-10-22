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
public class OPrefixBTreeBucket<V> extends ODurablePage {
  private static final int BUCKET_PREFIX_OFFSET = NEXT_FREE_POSITION;

  private int freePointerOffset;
  private int sizeOffset;
  private int isLeafOffset;

  private int treeSizeOffset;

  private int positionsArrayOffset;

  private final boolean isLeaf;

  private final OBinarySerializer<String> keySerializer;
  private final OBinarySerializer<V>      valueSerializer;

  private final Comparator<? super Object> comparator = ODefaultComparator.INSTANCE;

  private final OEncryption encryption;
  private       String      bucketPrefix;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  OPrefixBTreeBucket(OCacheEntry cacheEntry, boolean isLeaf, OBinarySerializer<String> keySerializer,
      OBinarySerializer<V> valueSerializer, OEncryption encryption, String bucketPrefix) {
    super(cacheEntry);

    this.isLeaf = isLeaf;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.encryption = encryption;

    final int bucketPrefixSize;

    if (encryption == null) {
      final byte[] serializedPrefix = keySerializer.serializeNativeAsWhole(bucketPrefix);
      setBinaryValue(BUCKET_PREFIX_OFFSET, serializedPrefix);
      bucketPrefixSize = serializedPrefix.length;
    } else {
      final byte[] serializedPrefix = keySerializer.serializeNativeAsWhole(bucketPrefix);
      final byte[] encryptedPrefix = encryption.encrypt(serializedPrefix);
      setIntValue(BUCKET_PREFIX_OFFSET, encryptedPrefix.length);
      setBinaryValue(BUCKET_PREFIX_OFFSET + OIntegerSerializer.INT_SIZE, encryptedPrefix);
      bucketPrefixSize = encryptedPrefix.length + OIntegerSerializer.INT_SIZE;
    }

    this.bucketPrefix = bucketPrefix;

    calculateOffsets(bucketPrefixSize);

    setIntValue(freePointerOffset, MAX_PAGE_SIZE_BYTES);
    setIntValue(sizeOffset, 0);

    setByteValue(isLeafOffset, (byte) (isLeaf ? 1 : 0));

    setLongValue(treeSizeOffset, 0);
  }

  private void calculateOffsets(int bucketPrefixSize) {
    freePointerOffset = bucketPrefixSize + BUCKET_PREFIX_OFFSET + OIntegerSerializer.INT_SIZE;
    sizeOffset = freePointerOffset + OIntegerSerializer.INT_SIZE;
    isLeafOffset = sizeOffset + OIntegerSerializer.INT_SIZE;
    treeSizeOffset = isLeafOffset + OByteSerializer.BYTE_SIZE;
    positionsArrayOffset = treeSizeOffset + OLongSerializer.LONG_SIZE;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public OPrefixBTreeBucket(OCacheEntry cacheEntry, OBinarySerializer<String> keySerializer, OBinarySerializer<V> valueSerializer,
      OEncryption encryption) {
    super(cacheEntry);
    this.encryption = encryption;

    final int commonPrefixSize;
    if (encryption == null) {
      commonPrefixSize = getObjectSizeInDirectMemory(keySerializer, BUCKET_PREFIX_OFFSET);
      bucketPrefix = deserializeFromDirectMemory(keySerializer, BUCKET_PREFIX_OFFSET);
    } else {
      final int encryptedSize = getIntValue(BUCKET_PREFIX_OFFSET);
      final byte[] encryptedKey = getBinaryValue(BUCKET_PREFIX_OFFSET + OIntegerSerializer.INT_SIZE, encryptedSize);
      final byte[] serializedPrefix = encryption.decrypt(encryptedKey);

      bucketPrefix = keySerializer.deserializeNativeObject(serializedPrefix, 0);
      commonPrefixSize = encryptedSize + OIntegerSerializer.INT_SIZE;
    }

    calculateOffsets(commonPrefixSize);

    this.isLeaf = getByteValue(isLeafOffset) > 0;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;

  }

  void setTreeSize(long size) {
    setLongValue(treeSizeOffset, size);
  }

  long getTreeSize() {
    return getLongValue(treeSizeOffset);
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public String getBucketPrefix() {
    return bucketPrefix;
  }

  public int find(String key) {
    final int size = size();

    int low = 0;
    int high = size - 1;

    if (key.length() < bucketPrefix.length()) {
      return -1;
    }

    if (!key.startsWith(bucketPrefix)) {
      if (key.compareTo(bucketPrefix) > 0) {
        return -(size + 1);
      }

      return -1;
    }

    key = key.substring(bucketPrefix.length());

    while (low <= high) {
      int mid = (low + high) >>> 1;
      String midVal = getKeyWithoutPrefix(mid);
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

  public void remove(final int entryIndex) {
    final int entryPosition = getIntValue(positionsArrayOffset + entryIndex * OIntegerSerializer.INT_SIZE);
    final int keySize;
    if (encryption == null) {
      keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      keySize = getIntValue(entryPosition) + OIntegerSerializer.INT_SIZE;
    }

    final int entrySize;
    if (isLeaf) {
      if (valueSerializer.isFixedLength()) {
        entrySize = keySize + valueSerializer.getFixedLength() + OByteSerializer.BYTE_SIZE;
      } else {
        assert getByteValue(entryPosition + keySize) == 0;
        entrySize = keySize + getObjectSizeInDirectMemory(valueSerializer, entryPosition + OByteSerializer.BYTE_SIZE + keySize)
            + OByteSerializer.BYTE_SIZE;
      }
    } else {
      throw new IllegalStateException("Remove is applies to leaf buckets only");
    }
    int size = getIntValue(sizeOffset);
    if (entryIndex < size - 1) {
      moveData(positionsArrayOffset + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          positionsArrayOffset + entryIndex * OIntegerSerializer.INT_SIZE, (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(sizeOffset, size);

    final int freePointer = getIntValue(freePointerOffset);
    if (size > 0 && entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    setIntValue(freePointerOffset, freePointer + entrySize);

    int currentPositionOffset = positionsArrayOffset;

    for (int i = 0; i < size; i++) {
      final int currentEntryPosition = getIntValue(currentPositionOffset);
      if (currentEntryPosition < entryPosition) {
        setIntValue(currentPositionOffset, currentEntryPosition + entrySize);
      }
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }
  }

  public int size() {
    return getIntValue(sizeOffset);
  }

  public SBTreeEntry<V> getEntry(int entryIndex) {
    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + positionsArrayOffset);

    if (isLeaf) {
      String key;
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

      boolean isLinkValue = getByteValue(entryPosition) > 0;
      V value;

      assert !isLinkValue;
      value = deserializeFromDirectMemory(valueSerializer, entryPosition + OByteSerializer.BYTE_SIZE);

      return new SBTreeEntry<>(-1, -1, bucketPrefix + key, new OSBTreeValue<>(false, -1, value));
    } else {
      int leftChild = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      int rightChild = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      String key;

      if (encryption == null) {
        key = deserializeFromDirectMemory(keySerializer, entryPosition);
      } else {
        final int encryptionSize = getIntValue(entryPosition);
        entryPosition += OIntegerSerializer.INT_SIZE;

        final byte[] encryptedKey = getBinaryValue(entryPosition, encryptionSize);

        final byte[] serializedKey = encryption.decrypt(encryptedKey);

        key = keySerializer.deserializeNativeObject(serializedKey, 0);
      }

      return new SBTreeEntry<>(leftChild, rightChild, bucketPrefix + key, null);
    }
  }

  public int getLeft(int entryIndex) {
    assert !isLeaf;

    final int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + positionsArrayOffset);
    return getIntValue(entryPosition);
  }

  public int getRight(int entryIndex) {
    assert !isLeaf;

    final int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + positionsArrayOffset);
    return getIntValue(entryPosition + OIntegerSerializer.INT_SIZE);
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

    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + positionsArrayOffset);

    // skip key
    if (encryption == null) {
      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedSize = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE + encryptedSize;
    }

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

    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + positionsArrayOffset);

    // skip key
    if (encryption == null) {
      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedSize = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE + encryptedSize;
    }

    assert getByteValue(entryPosition) == 0;

    final int valueSize = getObjectSizeInDirectMemory(valueSerializer, entryPosition + OByteSerializer.BYTE_SIZE);

    return getBinaryValue(entryPosition + OByteSerializer.BYTE_SIZE, valueSize);
  }

  String getKeyWithoutPrefix(int index) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + positionsArrayOffset);

    if (!isLeaf) {
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
    return isLeaf;
  }

  void addAllNoPrefix(final List<byte[]> rawEntries) {
    for (int i = 0; i < rawEntries.size(); i++) {
      appendRawEntry(i, rawEntries.get(i));
    }

    setIntValue(sizeOffset, rawEntries.size());
  }

  void addAllWithPrefix(List<SBTreeEntry<V>> entries, String bucketPrefix) {
    final int bucketPrefixSize;

    if (encryption == null) {
      final byte[] serializedPrefix = keySerializer.serializeNativeAsWhole(bucketPrefix);
      setBinaryValue(BUCKET_PREFIX_OFFSET, serializedPrefix);
      bucketPrefixSize = serializedPrefix.length;
    } else {
      final byte[] serializedPrefix = keySerializer.serializeNativeAsWhole(bucketPrefix);
      final byte[] encryptedPrefix = encryption.encrypt(serializedPrefix);
      setIntValue(BUCKET_PREFIX_OFFSET, encryptedPrefix.length);
      setBinaryValue(BUCKET_PREFIX_OFFSET + OIntegerSerializer.INT_SIZE, encryptedPrefix);
      bucketPrefixSize = encryptedPrefix.length + OIntegerSerializer.INT_SIZE;
    }

    calculateOffsets(bucketPrefixSize);

    setIntValue(freePointerOffset, MAX_PAGE_SIZE_BYTES);
    setIntValue(sizeOffset, 0);

    setByteValue(isLeafOffset, (byte) (isLeaf ? 1 : 0));

    setLongValue(treeSizeOffset, 0);

    this.bucketPrefix = bucketPrefix;

    for (int i = 0; i < entries.size(); i++) {
      final SBTreeEntry<V> entry = entries.get(i);
      addEntry(i, entry, false);
    }
  }

  void shrinkWithPrefix(final int newSize, final String bucketPrefix) {
    List<SBTreeEntry<V>> treeEntries = new ArrayList<>(newSize);

    for (int i = 0; i < newSize; i++) {
      treeEntries.add(getEntry(i));
    }

    final long treeSize = getLongValue(treeSizeOffset);

    final int bucketPrefixSize;

    if (encryption == null) {
      final byte[] serializedPrefix = keySerializer.serializeNativeAsWhole(bucketPrefix);
      setBinaryValue(BUCKET_PREFIX_OFFSET, serializedPrefix);
      bucketPrefixSize = serializedPrefix.length;
    } else {
      final byte[] serializedPrefix = keySerializer.serializeNativeAsWhole(bucketPrefix);
      final byte[] encryptedPrefix = encryption.encrypt(serializedPrefix);
      setIntValue(BUCKET_PREFIX_OFFSET, encryptedPrefix.length);
      setBinaryValue(BUCKET_PREFIX_OFFSET + OIntegerSerializer.INT_SIZE, encryptedPrefix);
      bucketPrefixSize = encryptedPrefix.length + OIntegerSerializer.INT_SIZE;
    }

    calculateOffsets(bucketPrefixSize);

    this.bucketPrefix = bucketPrefix;

    setIntValue(freePointerOffset, MAX_PAGE_SIZE_BYTES);
    setIntValue(sizeOffset, 0);

    setByteValue(isLeafOffset, (byte) (isLeaf ? 1 : 0));

    setLongValue(treeSizeOffset, treeSize);

    int index = 0;
    for (SBTreeEntry<V> entry : treeEntries) {
      addEntry(index, entry, false);
      index++;
    }
  }

  byte[] getRawEntry(final int entryIndex) {
    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + positionsArrayOffset);
    final int startEntryPosition = entryPosition;

    if (isLeaf) {
      final int keySize;
      if (encryption == null) {
        keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
      } else {
        final int encryptedSize = getIntValue(entryPosition);
        keySize = OIntegerSerializer.INT_SIZE + encryptedSize;
      }

      entryPosition += keySize;

      assert getByteValue(entryPosition) == 0;

      final int valueSize = getObjectSizeInDirectMemory(valueSerializer, entryPosition + OByteSerializer.BYTE_SIZE);

      return getBinaryValue(startEntryPosition, keySize + valueSize + OByteSerializer.BYTE_SIZE);
    } else {
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

  private void appendRawEntry(final int index, final byte[] rawEntry) {
    int freePointer = getIntValue(freePointerOffset);
    freePointer -= rawEntry.length;

    setIntValue(freePointerOffset, freePointer);
    setIntValue(positionsArrayOffset + index * OIntegerSerializer.INT_SIZE, freePointer);

    setBinaryValue(freePointer, rawEntry);
  }

  public boolean addEntry(int index, SBTreeEntry<V> treeEntry, boolean updateNeighbors) {
    assert treeEntry.key.startsWith(bucketPrefix);
    final String key = treeEntry.key.substring(bucketPrefix.length());

    final byte[] serializedKey = keySerializer.serializeNativeAsWhole(key, OType.STRING);
    final int keySize;
    byte[] encryptedKey = null;

    if (encryption == null) {
      keySize = serializedKey.length;
    } else {
      encryptedKey = encryption.encrypt(serializedKey);
      keySize = encryptedKey.length + OIntegerSerializer.INT_SIZE;
    }

    int valueSize = 0;
    int entrySize = keySize;

    if (isLeaf) {
      if (valueSerializer.isFixedLength()) {
        valueSize = valueSerializer.getFixedLength();
      } else {
        valueSize = valueSerializer.getObjectSize(treeEntry.value.getValue());
      }

      entrySize += valueSize + OByteSerializer.BYTE_SIZE;
    } else {
      entrySize += 2 * OIntegerSerializer.INT_SIZE;
    }

    int size = size();
    int freePointer = getIntValue(freePointerOffset);

    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + positionsArrayOffset) {
      return false;
    }

    if (index <= size - 1) {
      moveData(positionsArrayOffset + index * OIntegerSerializer.INT_SIZE,
          positionsArrayOffset + (index + 1) * OIntegerSerializer.INT_SIZE, (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(freePointerOffset, freePointer);
    setIntValue(positionsArrayOffset + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(sizeOffset, size + 1);

    if (isLeaf) {
      if (encryption == null) {
        freePointer += setBinaryValue(freePointer, serializedKey);
      } else {
        setIntValue(freePointer, encryptedKey.length);
        freePointer += OIntegerSerializer.INT_SIZE;

        freePointer += setBinaryValue(freePointer, encryptedKey);
      }

      freePointer += setByteValue(freePointer, treeEntry.value.isLink() ? (byte) 1 : (byte) 0);

      byte[] serializedValue = valueSerializer.serializeNativeAsWhole(treeEntry.value.getValue());

      setBinaryValue(freePointer, serializedValue);
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
          final int nextEntryPosition = getIntValue(positionsArrayOffset + (index + 1) * OIntegerSerializer.INT_SIZE);
          setIntValue(nextEntryPosition, treeEntry.rightChild);
        }

        if (index > 0) {
          final int prevEntryPosition = getIntValue(positionsArrayOffset + (index - 1) * OIntegerSerializer.INT_SIZE);
          setIntValue(prevEntryPosition + OIntegerSerializer.INT_SIZE, treeEntry.leftChild);
        }
      }
    }

    return true;
  }

  void updateValue(final int index, final byte[] value) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + positionsArrayOffset);

    if (encryption == null) {
      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedValue = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE + encryptedValue;
    }

    assert getByteValue(entryPosition) == 0;

    entryPosition += OByteSerializer.BYTE_SIZE;

    setBinaryValue(entryPosition, value);
  }

  public static final class SBTreeEntry<V> implements Comparable<SBTreeEntry<V>> {
    private final Comparator<? super String> comparator = ODefaultComparator.INSTANCE;

    final        int             leftChild;
    final        int             rightChild;
    public       String          key;
    public final OSBTreeValue<V> value;

    public SBTreeEntry(int leftChild, int rightChild, String key, OSBTreeValue<V> value) {
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
      SBTreeEntry<?> that = (SBTreeEntry<?>) o;
      return leftChild == that.leftChild && rightChild == that.rightChild && Objects.equals(comparator, that.comparator) && Objects
          .equals(key, that.key) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(comparator, leftChild, rightChild, key, value);
    }

    @Override
    public String toString() {
      return "SBTreeEntry{" + "leftChild=" + leftChild + ", rightChild=" + rightChild + ", key=" + key + ", value=" + value + '}';
    }

    @Override
    public int compareTo(SBTreeEntry<V> other) {
      return comparator.compare(key, other.key);
    }
  }
}
