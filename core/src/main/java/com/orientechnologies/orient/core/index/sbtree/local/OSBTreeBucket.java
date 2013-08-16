package com.orientechnologies.orient.core.index.sbtree.local;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OAbstractPLocalPage;

/**
 * @author Andrey Lomakin
 * @since 8/7/13
 */
public class OSBTreeBucket<K> extends OAbstractPLocalPage {
  private static final int            FREE_POINTER_OFFSET    = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int            SIZE_OFFSET            = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int            IS_LEAF_OFFSET         = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int            LEFT_SIBLING_OFFSET    = IS_LEAF_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int            RIGHT_SIBLING_OFFSET   = LEFT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int            TREE_SIZE_OFFSET       = RIGHT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int            KEY_SIZE_OFFSET        = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int            KEY_SERIALIZER_OFFSET  = KEY_SIZE_OFFSET + OByteSerializer.BYTE_SIZE;

  private static final int            POSITIONS_ARRAY_OFFSET = KEY_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;

  public static final int             MAX_BUCKET_SIZE_BYTES  = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  private final boolean               isLeaf;

  private final OBinarySerializer<K>  keySerializer;
  private final Comparator<? super K> comparator             = ODefaultComparator.INSTANCE;

  public OSBTreeBucket(long cachePointer, boolean isLeaf, OBinarySerializer<K> keySerializer, TrackMode trackMode)
      throws IOException {
    super(cachePointer, trackMode);

    this.isLeaf = isLeaf;
    this.keySerializer = keySerializer;

    setIntValue(FREE_POINTER_OFFSET, MAX_BUCKET_SIZE_BYTES);
    setIntValue(SIZE_OFFSET, 0);

    setByteValue(IS_LEAF_OFFSET, (byte) (isLeaf ? 1 : 0));
    setLongValue(LEFT_SIBLING_OFFSET, -1);
    setLongValue(RIGHT_SIBLING_OFFSET, -1);

    setLongValue(TREE_SIZE_OFFSET, 0);
    setByteValue(KEY_SIZE_OFFSET, (byte) -1);
  }

  public OSBTreeBucket(long cachePointer, OBinarySerializer<K> keySerializer, TrackMode trackMode) {
    super(cachePointer, trackMode);

    this.isLeaf = getByteValue(IS_LEAF_OFFSET) > 0;
    this.keySerializer = keySerializer;
  }

  public byte getKeySize() {
    return getByteValue(KEY_SIZE_OFFSET);
  }

  public void setKeySize(byte keySize) {
    setByteValue(KEY_SIZE_OFFSET, keySize);
  }

  public byte getKeySerializerId() {
    return getByteValue(KEY_SERIALIZER_OFFSET);
  }

  public void setKeySerializerId(byte keySerializerId) {
    setByteValue(KEY_SERIALIZER_OFFSET, keySerializerId);
  }

  public void setTreeSize(long size) throws IOException {
    setLongValue(TREE_SIZE_OFFSET, size);
  }

  public long getTreeSize() {
    return getLongValue(TREE_SIZE_OFFSET);
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
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);

    int entrySize = keySerializer.getObjectSizeInDirectMemory(directMemory, pagePointer + entryPosition);
    if (isLeaf) {
      entrySize += OLinkSerializer.RID_SIZE;
    } else {
      throw new IllegalStateException("Remove is applies to leaf buckets only");
    }

    int size = size();
    if (entryIndex < size - 1) {
      copyData(POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE, POSITIONS_ARRAY_OFFSET + entryIndex
          * OIntegerSerializer.INT_SIZE, (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(SIZE_OFFSET, size);

    if (size > 0) {
      int freePointer = getIntValue(FREE_POINTER_OFFSET);

      if (entryPosition > freePointer) {
        copyData(freePointer, freePointer + entrySize, entryPosition - freePointer);
      }

      setIntValue(FREE_POINTER_OFFSET, freePointer + entrySize);
    }

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      int currentEntryPosition = getIntValue(currentPositionOffset);
      if (currentEntryPosition < entryPosition)
        setIntValue(currentPositionOffset, currentEntryPosition + entrySize);
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }
  }

  public int size() {
    return getIntValue(SIZE_OFFSET);
  }

  public SBTreeEntry<K> getEntry(int entryIndex) {
    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (isLeaf) {
      K key = keySerializer.deserializeFromDirectMemory(directMemory, pagePointer + entryPosition);
      entryPosition += keySerializer.getObjectSizeInDirectMemory(directMemory, pagePointer + entryPosition);

      ORID value = OLinkSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryPosition).getIdentity();

      return new SBTreeEntry<K>(-1, -1, key, value);
    } else {
      long leftChild = getLongValue(entryPosition);
      entryPosition += OLongSerializer.LONG_SIZE;

      long rightChild = getLongValue(entryPosition);
      entryPosition += OLongSerializer.LONG_SIZE;

      K key = keySerializer.deserializeFromDirectMemory(directMemory, pagePointer + entryPosition);

      return new SBTreeEntry<K>(leftChild, rightChild, key, null);
    }
  }

  public K getKey(int index) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf)
      entryPosition += 2 * OLongSerializer.LONG_SIZE;

    return keySerializer.deserializeFromDirectMemory(directMemory, pagePointer + entryPosition);
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  public void addAll(List<SBTreeEntry<K>> entries) throws IOException {
    for (int i = 0; i < entries.size(); i++)
      addEntry(i, entries.get(i), false);
  }

  public void shrink(int newSize) throws IOException {
    List<SBTreeEntry<K>> treeEntries = new ArrayList<SBTreeEntry<K>>(newSize);

    for (int i = 0; i < newSize; i++) {
      treeEntries.add(getEntry(i));
    }

    setIntValue(FREE_POINTER_OFFSET, MAX_BUCKET_SIZE_BYTES);
    setIntValue(SIZE_OFFSET, 0);

    int index = 0;
    for (SBTreeEntry<K> entry : treeEntries) {
      addEntry(index, entry, false);
      index++;
    }
  }

  public boolean addEntry(int index, SBTreeEntry<K> treeEntry, boolean updateNeighbors) throws IOException {
    final int keySize = keySerializer.getObjectSize(treeEntry.key);
    int entrySize = keySize;

    if (isLeaf)
      entrySize += OLinkSerializer.RID_SIZE;
    else
      entrySize += 2 * OLongSerializer.LONG_SIZE;

    int size = size();
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET)
      return false;

    if (index <= size - 1) {
      copyData(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, POSITIONS_ARRAY_OFFSET + (index + 1)
          * OIntegerSerializer.INT_SIZE, (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    if (isLeaf) {
      byte[] serializedKey = new byte[keySize];
      keySerializer.serializeNative(treeEntry.key, serializedKey, 0);

      setBinaryValue(freePointer, serializedKey);
      freePointer += keySize;

      byte[] serializedLink = new byte[OLinkSerializer.RID_SIZE];
      OLinkSerializer.INSTANCE.serializeNative(treeEntry.value, serializedLink, 0);
      setBinaryValue(freePointer, serializedLink);

    } else {
      setLongValue(freePointer, treeEntry.leftChild);
      freePointer += OLongSerializer.LONG_SIZE;

      setLongValue(freePointer, treeEntry.rightChild);
      freePointer += OLongSerializer.LONG_SIZE;

      byte[] serializedKey = new byte[keySize];
      keySerializer.serializeNative(treeEntry.key, serializedKey, 0);
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

  public void updateValue(int index, ORID value) throws IOException {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    entryPosition += keySerializer.getObjectSizeInDirectMemory(directMemory, pagePointer + entryPosition);

    byte[] serializeLink = new byte[OLinkSerializer.RID_SIZE];
    OLinkSerializer.INSTANCE.serializeNative(value, serializeLink, 0);

    setBinaryValue(entryPosition, serializeLink);
  }

  public void setLeftSibling(long pageIndex) throws IOException {
    setLongValue(LEFT_SIBLING_OFFSET, pageIndex);
  }

  public long getLeftSibling() {
    return getLongValue(LEFT_SIBLING_OFFSET);
  }

  public void setRightSibling(long pageIndex) throws IOException {
    setLongValue(RIGHT_SIBLING_OFFSET, pageIndex);
  }

  public long getRightSibling() {
    return getLongValue(RIGHT_SIBLING_OFFSET);
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
