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
package com.orientechnologies.orient.core.storage.impl.local.eh;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.version.OVersionFactory;

/**
 * @author Andrey Lomakin
 * @since 06.02.13
 */
public class OEHBucket implements Iterable<OPhysicalPosition> {
  public static final int               MAX_BUCKET_SIZE = 256;

  private final int                     keySize;

  private final int                     entreeSize;

  private static final int              depthOffset     = 0;
  private static final int              sizeOffset      = OByteSerializer.BYTE_SIZE;

  private static final int              keyLineOffset   = sizeOffset + OIntegerSerializer.INT_SIZE;

  private final int                     entriesOffset;

  private final int                     historyOffset;

  private final int                     nextRemovedBucket;

  public final int                      bufferSize;

  private final byte[]                  dataBuffer;

  private final int                     dataBufferOffset;

  private int                           size            = -1;

  private final OClusterPositionFactory clusterPositionFactory;

  public OEHBucket(byte[] dataBuffer, int dataBufferOffset, int keySize, int entreeSize,
      OClusterPositionFactory clusterPositionFactory) {
    this.keySize = keySize;
    this.entreeSize = entreeSize;
    this.clusterPositionFactory = clusterPositionFactory;

    final BucketOffsets bucketOffsets = calculateBufferSize(keySize, entreeSize);

    entriesOffset = bucketOffsets.entriesOffset;
    historyOffset = bucketOffsets.historyOffset;
    nextRemovedBucket = bucketOffsets.nextRemovedBucket;
    bufferSize = bucketOffsets.bufferSize;

    this.dataBuffer = dataBuffer;
    this.dataBufferOffset = dataBufferOffset;
  }

  public OEHBucket(final OEHBucket parent, byte[] dataBuffer, int dataBufferOffset, int keySize, int entreeSize,
      OClusterPositionFactory clusterPositionFactory) {
    this(dataBuffer, dataBufferOffset, keySize, entreeSize, clusterPositionFactory);

    System.arraycopy(parent.dataBuffer, parent.dataBufferOffset + historyOffset, dataBuffer, dataBufferOffset + historyOffset,
        64 * OLongSerializer.LONG_SIZE);
    dataBuffer[dataBufferOffset + depthOffset] = parent.dataBuffer[parent.dataBufferOffset + depthOffset];
  }

  public OEHBucket(int depth, byte[] dataBuffer, int dataBufferOffset, int keySize, int entreeSize,
      OClusterPositionFactory clusterPositionFactory) {
    this(dataBuffer, dataBufferOffset, keySize, entreeSize, clusterPositionFactory);

    dataBuffer[dataBufferOffset + depthOffset] = (byte) depth;
  }

  public OPhysicalPosition find(final OClusterPosition key) {
    final int index = binarySearch(key);
    if (index < 0)
      return null;

    return getEntry(index);
  }

  private int binarySearch(OClusterPosition key) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      OClusterPosition midVal = getKey(mid);
      int cmp = midVal.compareTo(key);

      if (cmp < 0)
        low = mid + 1;
      else if (cmp > 0)
        high = mid - 1;
      else
        return mid; // key found
    }
    return -(low + 1); // key not found.
  }

  public OPhysicalPosition getEntry(int index) {
    final OPhysicalPosition position = new OPhysicalPosition();

    int bufferPosition = dataBufferOffset + entriesOffset + index * entreeSize;
    position.clusterPosition = clusterPositionFactory.fromStream(dataBuffer, bufferPosition);
    bufferPosition += clusterPositionFactory.getSerializedSize();

    position.dataSegmentId = OIntegerSerializer.INSTANCE.deserializeNative(dataBuffer, bufferPosition);
    bufferPosition += OIntegerSerializer.INT_SIZE;

    position.dataSegmentPos = OLongSerializer.INSTANCE.deserializeNative(dataBuffer, bufferPosition);
    bufferPosition += OLongSerializer.LONG_SIZE;

    position.recordType = dataBuffer[bufferPosition];
    bufferPosition++;

    position.recordVersion = OVersionFactory.instance().createVersion();
    position.recordVersion.getSerializer().fastReadFrom(dataBuffer, bufferPosition, position.recordVersion);
    bufferPosition += OVersionFactory.instance().getVersionSize();

    position.recordSize = OIntegerSerializer.INSTANCE.deserializeNative(dataBuffer, bufferPosition);

    return position;
  }

  public OClusterPosition getKey(int index) {
    final int bufferPosition = index * keySize + keyLineOffset + dataBufferOffset;
    return clusterPositionFactory.fromStream(dataBuffer, bufferPosition);
  }

  public int getIndex(final OClusterPosition key) {
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

  public Iterator<OPhysicalPosition> iterator() {
    return new EntryIterator(0);
  }

  public Iterator<OPhysicalPosition> iterator(int index) {
    return new EntryIterator(index);
  }

  public void deleteEntry(int index) {
    System.arraycopy(dataBuffer, (index + 1) * keySize + keyLineOffset + dataBufferOffset, dataBuffer, index * keySize
        + keyLineOffset + dataBufferOffset, size() * keySize - (index + 1) * keySize);

    System.arraycopy(dataBuffer, (index + 1) * entreeSize + entriesOffset + dataBufferOffset, dataBuffer, index * entreeSize
        + entriesOffset + dataBufferOffset, size() * entreeSize - (index + 1) * entreeSize);
    size--;
  }

  public void addEntry(final OPhysicalPosition value) {
    if (MAX_BUCKET_SIZE - size() <= 0)
      throw new IllegalArgumentException("There is no enough space in bucket.");

    final int index = binarySearch(value.clusterPosition);

    if (index >= 0)
      throw new IllegalArgumentException("Given value is present in bucket.");

    final int insertionPoint = -index - 1;
    System.arraycopy(dataBuffer, insertionPoint * keySize + keyLineOffset + dataBufferOffset, dataBuffer, (insertionPoint + 1)
        * keySize + keyLineOffset + dataBufferOffset, size() * keySize - insertionPoint * keySize);

    System.arraycopy(dataBuffer, insertionPoint * entreeSize + entriesOffset + dataBufferOffset, dataBuffer, (insertionPoint + 1)
        * entreeSize + entriesOffset + dataBufferOffset, size() * entreeSize - insertionPoint * entreeSize);

    serializeEntry(value, insertionPoint);

    size++;
  }

  public void appendEntry(final OPhysicalPosition value) {
    if (MAX_BUCKET_SIZE - size() <= 0)
      throw new IllegalArgumentException("There is no enough space in bucket.");

    serializeEntry(value, size());
    size++;
  }

  private void serializeEntry(OPhysicalPosition value, int insertionPoint) {
    final byte[] serializedKey = value.clusterPosition.toStream();

    System.arraycopy(serializedKey, 0, dataBuffer, insertionPoint * keySize + keyLineOffset + dataBufferOffset,
        serializedKey.length);

    int bufferPosition = dataBufferOffset + entriesOffset + insertionPoint * entreeSize;
    System.arraycopy(serializedKey, 0, dataBuffer, bufferPosition, serializedKey.length);

    bufferPosition += serializedKey.length;

    OIntegerSerializer.INSTANCE.serializeNative(value.dataSegmentId, dataBuffer, bufferPosition);
    bufferPosition += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serializeNative(value.dataSegmentPos, dataBuffer, bufferPosition);
    bufferPosition += OLongSerializer.LONG_SIZE;

    dataBuffer[bufferPosition] = value.recordType;
    bufferPosition++;

    value.recordVersion.getSerializer().fastWriteTo(dataBuffer, bufferPosition, value.recordVersion);
    bufferPosition += OVersionFactory.instance().getVersionSize();

    OIntegerSerializer.INSTANCE.serializeNative(value.recordSize, dataBuffer, bufferPosition);
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

  public void save() {
    if (size() > -1)
      OIntegerSerializer.INSTANCE.serializeNative(size, dataBuffer, dataBufferOffset + sizeOffset);
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

  private final class EntryIterator implements Iterator<OPhysicalPosition> {
    private int currentIndex;

    private EntryIterator(int currentIndex) {
      this.currentIndex = currentIndex;
    }

    @Override
    public boolean hasNext() {
      return currentIndex < size;
    }

    @Override
    public OPhysicalPosition next() {
      if (currentIndex >= size)
        throw new NoSuchElementException("Iterator was reached last element");

      final OPhysicalPosition position = getEntry(currentIndex);
      currentIndex++;
      return position;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove operation is not supported");
    }
  }
}
