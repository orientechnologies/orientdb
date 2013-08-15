package com.orientechnologies.orient.core.index.sbtree;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;

/**
 * @author Andrey Lomakin
 * @since 8/7/13
 */
public class OSBTreeBucket<K> {
  private final ODirectMemory         directMemory           = ODirectMemoryFactory.INSTANCE.directMemory();

  private static final int            MAGIC_NUMBER_OFFSET    = 0;
  private static final int            CRC32_OFFSET           = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int            WAL_SEGMENT_OFFSET     = CRC32_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int            WAL_POSITION_OFFSET    = WAL_SEGMENT_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int            FREE_POINTER_OFFSET    = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int            SIZE_OFFSET            = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int            IS_LEAF_OFFSET         = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int            LEFT_SIBLING_OFFSET    = IS_LEAF_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int            RIGHT_SIBLING_OFFSET   = LEFT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int            TREE_SIZE_OFFSET       = RIGHT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int            KEY_SERIALIZER_OFFSET  = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int            POSITIONS_ARRAY_OFFSET = KEY_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;

  public static final int             MAX_BUCKET_SIZE_BYTES  = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  private final boolean               isLeaf;
  private final long                  cachePointer;

  private final OBinarySerializer<K>  keySerializer;
  private final Comparator<? super K> comparator             = ODefaultComparator.INSTANCE;

  public OSBTreeBucket(long cachePointer, boolean isLeaf, OBinarySerializer<K> keySerializer) {
    this.isLeaf = isLeaf;
    this.cachePointer = cachePointer;
    this.keySerializer = keySerializer;

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(MAX_BUCKET_SIZE_BYTES, directMemory, cachePointer + FREE_POINTER_OFFSET);
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(0, directMemory, cachePointer + SIZE_OFFSET);
    directMemory.setByte(cachePointer + IS_LEAF_OFFSET, (byte) (isLeaf ? 1 : 0));

    OLongSerializer.INSTANCE.serializeInDirectMemory(-1L, directMemory, cachePointer + LEFT_SIBLING_OFFSET);
    OLongSerializer.INSTANCE.serializeInDirectMemory(-1L, directMemory, cachePointer + RIGHT_SIBLING_OFFSET);

    OLongSerializer.INSTANCE.serializeInDirectMemory(0L, directMemory, cachePointer + TREE_SIZE_OFFSET);
    directMemory.setByte(cachePointer + KEY_SERIALIZER_OFFSET, (byte) -1);
  }

  public OSBTreeBucket(long cachePointer, OBinarySerializer<K> keySerializer) {
    this.isLeaf = directMemory.getByte(cachePointer + IS_LEAF_OFFSET) > 0;
    this.cachePointer = cachePointer;
    this.keySerializer = keySerializer;
  }

  public byte getKeySerializerId() {
    return directMemory.getByte(cachePointer + KEY_SERIALIZER_OFFSET);
  }

  public void setKeySerializerId(byte keySerializerId) {
    directMemory.setByte(cachePointer + KEY_SERIALIZER_OFFSET, keySerializerId);
  }

  public void setTreeSize(long size) {
    OLongSerializer.INSTANCE.serializeInDirectMemory(size, directMemory, cachePointer + TREE_SIZE_OFFSET);
  }

  public long getTreeSize() {
    return OLongSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, cachePointer + TREE_SIZE_OFFSET);
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
    int entryPosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, cachePointer + POSITIONS_ARRAY_OFFSET
        + entryIndex * OIntegerSerializer.INT_SIZE);

    int entrySize = keySerializer.getObjectSizeInDirectMemory(directMemory, cachePointer + entryPosition);
    if (isLeaf) {
      entrySize += OLinkSerializer.RID_SIZE;
    } else {
      throw new IllegalStateException("Remove is applies to leaf buckets only");
    }

    int size = size();
    if (entryIndex < size - 1) {
      directMemory.copyData(cachePointer + POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE, cachePointer
          + POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE, (size - entryIndex - 1)
          * OIntegerSerializer.INT_SIZE);
    }

    size--;
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(size, directMemory, cachePointer + SIZE_OFFSET);

    if (size > 0) {
      int freePointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, cachePointer + FREE_POINTER_OFFSET);
      if (entryPosition > freePointer) {
        directMemory.copyData(cachePointer + freePointer, cachePointer + freePointer + entrySize, entryPosition - freePointer);
      }
      OIntegerSerializer.INSTANCE
          .serializeInDirectMemory(freePointer + entrySize, directMemory, cachePointer + FREE_POINTER_OFFSET);
    }

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      int currentEntryPosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, cachePointer
          + currentPositionOffset);
      if (currentEntryPosition < entryPosition)
        OIntegerSerializer.INSTANCE.serializeInDirectMemory(currentEntryPosition + entrySize, directMemory, cachePointer
            + currentPositionOffset);
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }
  }

  public int size() {
    return OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, cachePointer + SIZE_OFFSET);
  }

  public SBTreeEntry<K> getEntry(int entryIndex) {
    int entryPosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, cachePointer + entryIndex
        * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (isLeaf) {
      K key = keySerializer.deserializeFromDirectMemory(directMemory, cachePointer + entryPosition);
      entryPosition += keySerializer.getObjectSizeInDirectMemory(directMemory, cachePointer + entryPosition);

      ORID value = OLinkSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, cachePointer + entryPosition).getIdentity();

      return new SBTreeEntry<K>(-1, -1, key, value);
    } else {
      long leftChild = OLongSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, cachePointer + entryPosition);
      entryPosition += OLongSerializer.LONG_SIZE;

      long rightChild = OLongSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, cachePointer + entryPosition);
      entryPosition += OLongSerializer.LONG_SIZE;

      K key = keySerializer.deserializeFromDirectMemory(directMemory, cachePointer + entryPosition);

      return new SBTreeEntry<K>(leftChild, rightChild, key, null);
    }
  }

  public K getKey(int index) {
    int entryPosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, cachePointer + index
        * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf)
      entryPosition += 2 * OLongSerializer.LONG_SIZE;

    return keySerializer.deserializeFromDirectMemory(directMemory, cachePointer + entryPosition);
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  public void addAll(List<SBTreeEntry<K>> entries) {
    for (int i = 0; i < entries.size(); i++)
      addEntry(i, entries.get(i), false);
  }

  public void shrink(int newSize) {
    List<SBTreeEntry<K>> treeEntries = new ArrayList<SBTreeEntry<K>>(newSize);

    for (int i = 0; i < newSize; i++) {
      treeEntries.add(getEntry(i));
    }

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(MAX_BUCKET_SIZE_BYTES, directMemory, cachePointer + FREE_POINTER_OFFSET);
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(0, directMemory, cachePointer + SIZE_OFFSET);

    int index = 0;
    for (SBTreeEntry<K> entry : treeEntries) {
      addEntry(index, entry, false);
      index++;
    }
  }

  public boolean addEntry(int index, SBTreeEntry<K> treeEntry, boolean updateNeighbors) {
    final int keySize = keySerializer.getObjectSize(treeEntry.key);
    int entrySize = keySize;

    if (isLeaf)
      entrySize += OLinkSerializer.RID_SIZE;
    else
      entrySize += 2 * OLongSerializer.LONG_SIZE;

    int size = size();
    int freePointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, cachePointer + FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET)
      return false;

    if (index <= size - 1) {
      directMemory.copyData(cachePointer + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, cachePointer
          + POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE, (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(freePointer, directMemory, cachePointer + FREE_POINTER_OFFSET);
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(freePointer, directMemory, cachePointer + POSITIONS_ARRAY_OFFSET + index
        * OIntegerSerializer.INT_SIZE);
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(size + 1, directMemory, cachePointer + SIZE_OFFSET);

    if (isLeaf) {
      keySerializer.serializeInDirectMemory(treeEntry.key, directMemory, cachePointer + freePointer);
      freePointer += keySize;

      OLinkSerializer.INSTANCE.serializeInDirectMemory(treeEntry.value, directMemory, cachePointer + freePointer);
    } else {
      OLongSerializer.INSTANCE.serializeInDirectMemory(treeEntry.leftChild, directMemory, cachePointer + freePointer);
      freePointer += OLongSerializer.LONG_SIZE;

      OLongSerializer.INSTANCE.serializeInDirectMemory(treeEntry.rightChild, directMemory, cachePointer + freePointer);
      freePointer += OLongSerializer.LONG_SIZE;

      keySerializer.serializeInDirectMemory(treeEntry.key, directMemory, cachePointer + freePointer);

      size++;

      if (updateNeighbors && size > 1) {
        if (index < size - 1) {
          final int nextEntryPosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory,
              POSITIONS_ARRAY_OFFSET + cachePointer + (index + 1) * OIntegerSerializer.INT_SIZE);

          OLongSerializer.INSTANCE.serializeInDirectMemory(treeEntry.rightChild, directMemory, cachePointer + nextEntryPosition);
        }

        if (index > 0) {
          final int prevEntryPosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory,
              POSITIONS_ARRAY_OFFSET + cachePointer + (index - 1) * OIntegerSerializer.INT_SIZE);

          OLongSerializer.INSTANCE.serializeInDirectMemory(treeEntry.leftChild, directMemory, cachePointer + prevEntryPosition
              + OLongSerializer.LONG_SIZE);
        }
      }
    }

    return true;
  }

  public void updateValue(int index, ORID value) {
    int entryPosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, cachePointer + index
        * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    entryPosition += keySerializer.getObjectSizeInDirectMemory(directMemory, cachePointer + entryPosition);
    OLinkSerializer.INSTANCE.serializeInDirectMemory(value, directMemory, cachePointer + entryPosition);
  }

  public void setLeftSibling(long pageIndex) {
    OLongSerializer.INSTANCE.serializeInDirectMemory(pageIndex, directMemory, cachePointer + LEFT_SIBLING_OFFSET);
  }

  public long getLeftSibling() {
    return OLongSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, cachePointer + LEFT_SIBLING_OFFSET);
  }

  public void setRightSibling(long pageIndex) {
    OLongSerializer.INSTANCE.serializeInDirectMemory(pageIndex, directMemory, cachePointer + RIGHT_SIBLING_OFFSET);
  }

  public long getRightSibling() {
    return OLongSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, cachePointer + RIGHT_SIBLING_OFFSET);
  }

  public static final class SBTreeEntry<K> implements Comparable<SBTreeEntry<K>> {
    private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

    public final long                   leftChild;
    public final long                   rightChild;
    public final K                      key;
    public final ORID                   value;

    public SBTreeEntry(long leftChild, long rightChild, K key, ORID value) {
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

      SBTreeEntry that = (SBTreeEntry) o;

      if (leftChild != that.leftChild)
        return false;
      if (rightChild != that.rightChild)
        return false;
      if (!key.equals(that.key))
        return false;
      if (value != null ? !value.equals(that.value) : that.value != null)
        return false;

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
    public int compareTo(SBTreeEntry<K> other) {
      return comparator.compare(key, other.key);
    }
  }
}
