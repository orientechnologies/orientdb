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
  public static final int                        MAX_BUCKET_SIZE      = 256;

  private final int                              maxKeySize           = 1024;

  private final int                              entreeSize;

  private static final int                       depthOffset          = 0;
  private static final int                       sizeOffset           = OByteSerializer.BYTE_SIZE;
  private static final int                       positionsArrayOffset = sizeOffset + OIntegerSerializer.INT_SIZE;

  private static final int                       keyLineOffset        = positionsArrayOffset + MAX_BUCKET_SIZE
                                                                          * OIntegerSerializer.INT_SIZE;

  private final int                              entriesOffset;

  private final int                              historyOffset;

  private final int                              nextRemovedBucket;

  public final int                               bufferSize;

  private final byte[]                           dataBuffer;

  private final int                              dataBufferOffset;

  private int                                    size                 = -1;

  private final Comparator<byte[]>               arrayComparator      = OComparatorFactory.INSTANCE.getComparator(byte[].class);
  private final OBinarySerializer<OIdentifiable> ridSerializer        = OLinkSerializer.INSTANCE;
  private final OBinaryTypeSerializer            keySerializer        = OBinaryTypeSerializer.INSTANCE;

  public OHashIndexBucket(byte[] dataBuffer, int dataBufferOffset, int entreeSize) {
    this.entreeSize = entreeSize;

    final BucketOffsets bucketOffsets = calculateBufferSize(maxKeySize, entreeSize);

    entriesOffset = bucketOffsets.entriesOffset;
    historyOffset = bucketOffsets.historyOffset;
    nextRemovedBucket = bucketOffsets.nextRemovedBucket;
    bufferSize = bucketOffsets.bufferSize;

    this.dataBuffer = dataBuffer;
    this.dataBufferOffset = dataBufferOffset;

  }

  public OHashIndexBucket(final OHashIndexBucket parent, byte[] dataBuffer, int dataBufferOffset, int entreeSize) {
    this(dataBuffer, dataBufferOffset, entreeSize);

    System.arraycopy(parent.dataBuffer, parent.dataBufferOffset + historyOffset, dataBuffer, dataBufferOffset + historyOffset,
        64 * OLongSerializer.LONG_SIZE);
    dataBuffer[dataBufferOffset + depthOffset] = parent.dataBuffer[parent.dataBufferOffset + depthOffset];
  }

  public OHashIndexBucket(int depth, byte[] dataBuffer, int dataBufferOffset, int entreeSize) {
    this(dataBuffer, dataBufferOffset, entreeSize);

    dataBuffer[dataBufferOffset + depthOffset] = (byte) depth;
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
    final int positionIndex = OIntegerSerializer.INSTANCE.deserializeNative(dataBuffer, positionsArrayOffset + index
        * OIntegerSerializer.INT_SIZE + dataBufferOffset);

    int bufferPosition = dataBufferOffset + entriesOffset + positionIndex * entreeSize;
    final byte[] key = keySerializer.deserializeNative(dataBuffer, bufferPosition);
    bufferPosition += maxKeySize;

    final ORID rid = ridSerializer.deserializeNative(dataBuffer, bufferPosition).getIdentity();
    return new Entry(key, rid);
  }

  public byte[] getKey(int index) {
    final int positionIndex = OIntegerSerializer.INSTANCE.deserializeNative(dataBuffer, positionsArrayOffset + index
        * OIntegerSerializer.INT_SIZE + dataBufferOffset);
    final int bufferPosition = positionIndex * maxKeySize + keyLineOffset + dataBufferOffset;

    return keySerializer.deserializeNative(dataBuffer, bufferPosition);
  }

  public int getIndex(final byte[] key) {
    return binarySearch(key);
  }

  public int size() {
    if (size > -1)
      return size;

    size = OIntegerSerializer.INSTANCE.deserializeNative(dataBuffer, sizeOffset);
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
    final int positionOffset = positionsArrayOffset + index * OIntegerSerializer.INT_SIZE + dataBufferOffset;
    final int positionIndex = OIntegerSerializer.INSTANCE.deserializeNative(dataBuffer, positionOffset);
    System.arraycopy(dataBuffer, positionOffset + OIntegerSerializer.INT_SIZE, dataBuffer, positionOffset, size()
        * OIntegerSerializer.INT_SIZE - (index + 1) * OIntegerSerializer.INT_SIZE);

    System.arraycopy(dataBuffer, (positionIndex + 1) * maxKeySize + keyLineOffset + dataBufferOffset, dataBuffer, positionIndex
        * maxKeySize + keyLineOffset + dataBufferOffset, size() * maxKeySize - (positionIndex + 1) * maxKeySize);

    System.arraycopy(dataBuffer, (positionIndex + 1) * entreeSize + entriesOffset + dataBufferOffset, dataBuffer, positionIndex
        * entreeSize + entriesOffset + dataBufferOffset, size() * entreeSize - (positionIndex + 1) * entreeSize);

    size--;

    int currentPositionOffset = positionsArrayOffset + dataBufferOffset;
    for (int i = 0; i < size; i++) {
      int currentPosition = OIntegerSerializer.INSTANCE.deserializeNative(dataBuffer, currentPositionOffset);
      if (currentPosition > positionIndex)
        OIntegerSerializer.INSTANCE.serializeNative(currentPosition - 1, dataBuffer, currentPositionOffset);
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }
  }

  public void addEntry(byte[] key, ORID rid) {
    if (MAX_BUCKET_SIZE - size() <= 0)
      throw new IllegalArgumentException("There is no enough space in bucket.");

    final int serializedSize = keySerializer.getObjectSize(key);
    if (serializedSize > maxKeySize)
      throw new OIndexMaximumLimitReachedException("Maximum limit " + maxKeySize + " for key was reached.");

    final int index = binarySearch(key);

    if (index >= 0)
      throw new IllegalArgumentException("Given value is present in bucket.");

    final int insertionPoint = -index - 1;
    final int positionsOffset = insertionPoint * OIntegerSerializer.INT_SIZE + positionsArrayOffset + dataBufferOffset;
    System.arraycopy(dataBuffer, positionsOffset, dataBuffer, positionsOffset + OIntegerSerializer.INT_SIZE, size()
        * OIntegerSerializer.INT_SIZE - insertionPoint * OIntegerSerializer.INT_SIZE);

    OIntegerSerializer.INSTANCE.serializeNative(size(), dataBuffer, positionsOffset);
    serializeEntry(key, rid);

    size++;
  }

  public void appendEntry(byte[] key, ORID rid) {
    if (MAX_BUCKET_SIZE - size() <= 0)
      throw new IllegalArgumentException("There is no enough space in bucket.");

    final int positionsOffset = size() * OIntegerSerializer.INT_SIZE + positionsArrayOffset + dataBufferOffset;
    OIntegerSerializer.INSTANCE.serializeNative(size(), dataBuffer, positionsOffset);

    serializeEntry(key, rid);
    size++;
  }

  private void serializeEntry(byte[] key, ORID rid) {
    keySerializer.serializeNative(key, dataBuffer, size() * maxKeySize + keyLineOffset + dataBufferOffset);

    int bufferPosition = dataBufferOffset + entriesOffset + size() * entreeSize;
    keySerializer.serializeNative(key, dataBuffer, bufferPosition);

    bufferPosition += maxKeySize;

    ridSerializer.serializeNative(rid, dataBuffer, bufferPosition);
  }

  public int getDepth() {
    return dataBuffer[dataBufferOffset + depthOffset];
  }

  public void setDepth(int depth) {
    dataBuffer[dataBufferOffset + depthOffset] = (byte) depth;
  }

  public void setSplitHistory(int index, long position) {
    OLongSerializer.INSTANCE.serializeNative(position, dataBuffer, dataBufferOffset + historyOffset + index
        * OLongSerializer.LONG_SIZE);
  }

  public long getSplitHistory(int index) {
    return OLongSerializer.INSTANCE.deserializeNative(dataBuffer, dataBufferOffset + historyOffset + index
        * OLongSerializer.LONG_SIZE);

  }

  public long getNextRemovedBucketPair() {
    return OLongSerializer.INSTANCE.deserializeNative(dataBuffer, dataBufferOffset + nextRemovedBucket);
  }

  public void setNextRemovedBucketPair(long nextRemovedBucketPair) {
    OLongSerializer.INSTANCE.serializeNative(nextRemovedBucketPair, dataBuffer, dataBufferOffset + nextRemovedBucket);
  }

  public void toStream() {
    if (size > -1)
      OIntegerSerializer.INSTANCE.serializeNative(size, dataBuffer, dataBufferOffset + sizeOffset);

    size = -1;
  }

  public byte[] getDataBuffer() {
    return dataBuffer;
  }

  public static BucketOffsets calculateBufferSize(int keySize, int entreeSize) {
    final int entriesOffset = keyLineOffset + keySize * MAX_BUCKET_SIZE;
    final int historyOffset = entriesOffset + entreeSize * MAX_BUCKET_SIZE;
    final int nextRemovedBucket = historyOffset + 64 * OLongSerializer.LONG_SIZE;
    final int bufferSize = nextRemovedBucket + OLongSerializer.LONG_SIZE;

    return new BucketOffsets(entriesOffset, historyOffset, nextRemovedBucket, bufferSize);
  }

  public void updateEntry(int index, ORID rid) {
    final int bufferPosition = dataBufferOffset + entriesOffset + index * entreeSize + maxKeySize;
    ridSerializer.serializeNative(rid, dataBuffer, bufferPosition);
  }

  public static final class BucketOffsets {
    private final int entriesOffset;
    private final int historyOffset;
    private final int nextRemovedBucket;
    private final int bufferSize;

    public BucketOffsets(int entriesOffset, int historyOffset, int nextRemovedBucket, int bufferSize) {
      this.entriesOffset = entriesOffset;
      this.historyOffset = historyOffset;
      this.nextRemovedBucket = nextRemovedBucket;
      this.bufferSize = bufferSize;
    }

    public int getEntriesOffset() {
      return entriesOffset;
    }

    public int getHistoryOffset() {
      return historyOffset;
    }

    public int getNextRemovedBucket() {
      return nextRemovedBucket;
    }

    public int getBufferSize() {
      return bufferSize;
    }
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
