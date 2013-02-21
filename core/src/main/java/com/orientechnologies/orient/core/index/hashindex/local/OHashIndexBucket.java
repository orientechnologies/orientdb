package com.orientechnologies.orient.core.index.hashindex.local;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.orientechnologies.common.comparator.OComparatorFactory;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;

/**
 * @author Andrey Lomakin
 * @since 2/17/13
 */
public class OHashIndexBucket implements Iterable<OHashIndexBucket.Entry> {
  public static final int                        MAX_BUCKET_SIZE            = 256;

  private static final int                       MAX_KEY_SIZE               = 1024;

  private static final int                       DEPTH_OFFSET               = 0;
  private static final int                       SIZE_OFFSET                = OByteSerializer.BYTE_SIZE;

  private static final int                       POSITIONS_ARRAY_OFFSET     = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int                       NEXT_REMOVED_BUCKET_OFFSET = POSITIONS_ARRAY_OFFSET + OIntegerSerializer.INT_SIZE
                                                                                * MAX_BUCKET_SIZE;

  private static final int                       ENTRIES_OFFSET             = NEXT_REMOVED_BUCKET_OFFSET
                                                                                + OLongSerializer.LONG_SIZE;

  public static final int                        MAX_BUCKET_SIZE_BYTES      = ENTRIES_OFFSET + MAX_BUCKET_SIZE
                                                                                * (OLinkSerializer.RID_SIZE + MAX_KEY_SIZE);

  private byte[]                                 dataBuffer;

  private int                                    size                       = -1;
  private int                                    dataBufferLength;

  private final Comparator<byte[]>               arrayComparator            = OComparatorFactory.INSTANCE
                                                                                .getComparator(byte[].class);
  private final OBinarySerializer<OIdentifiable> ridSerializer              = OLinkSerializer.INSTANCE;
  private final OBinaryTypeSerializer            keySerializer              = OBinaryTypeSerializer.INSTANCE;

  public OHashIndexBucket(int depth) {
    dataBuffer = new byte[ENTRIES_OFFSET];
    dataBuffer[DEPTH_OFFSET] = (byte) depth;

    dataBufferLength = dataBuffer.length;
  }

  public OHashIndexBucket(byte[] dataBuffer) {
    this.dataBuffer = dataBuffer;

    dataBufferLength = dataBuffer.length;
  }

  public Entry find(final byte[] key) {
    final int index = binarySearch(key);
    if (index < 0)
      return null;

    return getEntry(index);
  }

  private int binarySearch(byte[] key) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      byte[] midVal = getKey(mid);
      int cmp = arrayComparator.compare(midVal, key);

      if (cmp < 0)
        low = mid + 1;
      else if (cmp > 0)
        high = mid - 1;
      else
        return mid; // key found
    }
    return -(low + 1); // key not found.
  }

  public Entry getEntry(int index) {
    int entryPosition = OIntegerSerializer.INSTANCE.deserializeNative(dataBuffer, POSITIONS_ARRAY_OFFSET + index
        * OIntegerSerializer.INT_SIZE);

    final byte[] key = keySerializer.deserializeNative(dataBuffer, entryPosition);
    entryPosition += keySerializer.getObjectSizeNative(dataBuffer, entryPosition);

    final ORID rid = ridSerializer.deserializeNative(dataBuffer, entryPosition).getIdentity();
    return new Entry(key, rid);
  }

  public byte[] getKey(int index) {
    int entryPosition = OIntegerSerializer.INSTANCE.deserializeNative(dataBuffer, POSITIONS_ARRAY_OFFSET + index
        * OIntegerSerializer.INT_SIZE);

    return keySerializer.deserializeNative(dataBuffer, entryPosition);
  }

  public int getIndex(final byte[] key) {
    return binarySearch(key);
  }

  public int size() {
    if (size > -1)
      return size;

    size = OIntegerSerializer.INSTANCE.deserializeNative(dataBuffer, SIZE_OFFSET);
    return size;
  }

  public void emptyBucket() {
    size = 0;
  }

  public Iterator<Entry> iterator() {
    return new EntryIterator(0);
  }

  public Iterator<Entry> iterator(int index) {
    return new EntryIterator(index);
  }

  public void deleteEntry(int index) {
    final int positionOffset = POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE;
    final int entryPosition = OIntegerSerializer.INSTANCE.deserializeNative(dataBuffer, positionOffset);
    final int keySize = keySerializer.getObjectSizeNative(dataBuffer, entryPosition);

    System.arraycopy(dataBuffer, positionOffset + OIntegerSerializer.INT_SIZE, dataBuffer, positionOffset, size()
        * OIntegerSerializer.INT_SIZE - (index + 1) * OIntegerSerializer.INT_SIZE);

    int nextEntryPosition = entryPosition + keySize;
    System.arraycopy(dataBuffer, nextEntryPosition, dataBuffer, entryPosition, dataBufferLength - nextEntryPosition);

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;
    for (int i = 0; i < size; i++) {
      int currentEntryPosition = OIntegerSerializer.INSTANCE.deserializeNative(dataBuffer, currentPositionOffset);
      if (currentEntryPosition > entryPosition)
        OIntegerSerializer.INSTANCE.serializeNative(currentEntryPosition - keySize, dataBuffer, currentPositionOffset);
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    dataBufferLength -= keySize;
    size--;
  }

  public void addEntry(byte[] key, ORID rid) {
    if (MAX_BUCKET_SIZE - size() <= 0)
      throw new IllegalArgumentException("There is no enough space in bucket.");

    final int serializedSize = keySerializer.getObjectSize(key);
    if (serializedSize > MAX_KEY_SIZE)
      throw new OIndexMaximumLimitReachedException("Maximum limit " + MAX_KEY_SIZE + " for key was reached.");

    final int index = binarySearch(key);

    if (index >= 0)
      throw new IllegalArgumentException("Given value is present in bucket.");

    final int insertionPoint = -index - 1;
    final int positionsOffset = insertionPoint * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;
    System.arraycopy(dataBuffer, positionsOffset, dataBuffer, positionsOffset + OIntegerSerializer.INT_SIZE, size()
        * OIntegerSerializer.INT_SIZE - insertionPoint * OIntegerSerializer.INT_SIZE);

    OIntegerSerializer.INSTANCE.serializeNative(dataBufferLength, dataBuffer, positionsOffset);
    serializeEntry(key, rid, dataBufferLength);

    int entreeSize = keySerializer.getObjectSize(key) + ridSerializer.getObjectSize(rid);
    dataBufferLength += entreeSize;

    size++;
  }

  public void appendEntry(byte[] key, ORID rid) {
    if (MAX_BUCKET_SIZE - size() <= 0)
      throw new IllegalArgumentException("There is no enough space in bucket.");

    final int positionsOffset = size() * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;

    OIntegerSerializer.INSTANCE.serializeNative(dataBufferLength, dataBuffer, positionsOffset);
    serializeEntry(key, rid, dataBufferLength);

    int entreeSize = keySerializer.getObjectSize(key) + ridSerializer.getObjectSize(rid);
    dataBufferLength += entreeSize;

    size++;
  }

  private void serializeEntry(byte[] key, ORID rid, int entryOffset) {
    int entreeSize = keySerializer.getObjectSize(key) + ridSerializer.getObjectSize(rid);
    if (entryOffset + entreeSize > dataBuffer.length) {
      byte[] oldDataBuffer = dataBuffer;
      dataBuffer = new byte[entryOffset + entreeSize];
      System.arraycopy(oldDataBuffer, 0, dataBuffer, 0, oldDataBuffer.length);
    }

    keySerializer.serializeNative(key, dataBuffer, entryOffset);
    entryOffset += keySerializer.getObjectSize(key);

    ridSerializer.serializeNative(rid, dataBuffer, entryOffset);
  }

  public int getDepth() {
    return dataBuffer[DEPTH_OFFSET];
  }

  public void setDepth(int depth) {
    dataBuffer[DEPTH_OFFSET] = (byte) depth;
  }

  public long getNextRemovedBucketPair() {
    return OLongSerializer.INSTANCE.deserializeNative(dataBuffer, NEXT_REMOVED_BUCKET_OFFSET);
  }

  public void setNextRemovedBucketPair(long nextRemovedBucketPair) {
    OLongSerializer.INSTANCE.serializeNative(nextRemovedBucketPair, dataBuffer, NEXT_REMOVED_BUCKET_OFFSET);
  }

  public void toStream() {
    if (size > -1)
      OIntegerSerializer.INSTANCE.serializeNative(size, dataBuffer, SIZE_OFFSET);

    size = -1;
  }

  public byte[] getDataBuffer() {
    return dataBuffer;
  }

  public int getDataBufferLength() {
    return dataBufferLength;
  }

  public void updateEntry(int index, ORID rid) {
    int entryPosition = OIntegerSerializer.INSTANCE.deserializeNative(dataBuffer, POSITIONS_ARRAY_OFFSET + index
        * OIntegerSerializer.INT_SIZE);

    entryPosition += keySerializer.getObjectSizeNative(dataBuffer, entryPosition);
    ridSerializer.serializeNative(rid, dataBuffer, entryPosition);
  }

  public static class Entry {
    public final byte[] key;
    public final ORID   rid;

    public Entry(byte[] key, ORID rid) {
      this.key = key;
      this.rid = rid;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      Entry entry = (Entry) o;

      if (!Arrays.equals(key, entry.key))
        return false;
      if (!rid.equals(entry.rid))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(key);
      result = 31 * result + rid.hashCode();
      return result;
    }
  }

  private final class EntryIterator implements Iterator<Entry> {
    private int currentIndex;

    private EntryIterator(int currentIndex) {
      this.currentIndex = currentIndex;
    }

    @Override
    public boolean hasNext() {
      return currentIndex < size();
    }

    @Override
    public Entry next() {
      if (currentIndex >= size())
        throw new NoSuchElementException("Iterator was reached last element");

      final Entry entry = getEntry(currentIndex);
      currentIndex++;
      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove operation is not supported");
    }
  }
}
