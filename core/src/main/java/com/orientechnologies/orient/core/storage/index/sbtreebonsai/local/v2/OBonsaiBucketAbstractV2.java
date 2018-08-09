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

package com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.v2;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.PageSerializationType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;

import java.nio.ByteBuffer;

/**
 * A base class for bonsai buckets. Bonsai bucket size is usually less than page size and one page could contain multiple bonsai
 * buckets.
 * Adds methods to read and write bucket pointers.
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 * @see OBonsaiBucketPointer
 * @see com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsai
 */
public class OBonsaiBucketAbstractV2 extends ODurablePage {
  private static final int BITS_IN_BYTE = 8;
  final                int maxBucketSizeInBytes;

  private static final int CREATED_BUCKETS_OFFSET = NEXT_FREE_POSITION;
  private final        int amountOfBuckets;
  private final        int bucketsFillSize;

  private final int endCreatedBuckets;

  final int freePointerOffset;
  final int sizeOffset;
  final int flagsOffset;
  final int freeListPointerOffset;
  final int leftSiblingOffset;
  final int rightSiblingOffset;
  final int treeSizeOffset;
  final int keySerializerOffset;
  final int valueSerializerOffset;
  final int positionsArrayOffset;

  final int sysMagicOffset;
  final int freeSpaceOffset;
  final int freeListHeadOffset;
  final int freeListLengthOffset;

  public OBonsaiBucketAbstractV2(OCacheEntry cacheEntry, int maxBucketSizeInBytes) {
    super(cacheEntry);

    this.maxBucketSizeInBytes = maxBucketSizeInBytes;
    this.amountOfBuckets = PAGE_SIZE / maxBucketSizeInBytes;
    this.bucketsFillSize = (amountOfBuckets + BITS_IN_BYTE - 1) / BITS_IN_BYTE;
    this.endCreatedBuckets = CREATED_BUCKETS_OFFSET + bucketsFillSize;

    this.freePointerOffset = endCreatedBuckets;
    this.sizeOffset = freePointerOffset + OIntegerSerializer.INT_SIZE;
    this.flagsOffset = sizeOffset + OIntegerSerializer.INT_SIZE;
    this.freeListPointerOffset = flagsOffset + OByteSerializer.BYTE_SIZE;
    this.leftSiblingOffset = freeListPointerOffset + 2 * OIntegerSerializer.INT_SIZE;
    this.rightSiblingOffset = leftSiblingOffset + 2 * OIntegerSerializer.INT_SIZE;
    this.treeSizeOffset = rightSiblingOffset + 2 * OIntegerSerializer.INT_SIZE;
    this.keySerializerOffset = treeSizeOffset + OLongSerializer.LONG_SIZE;
    this.valueSerializerOffset = keySerializerOffset + OByteSerializer.BYTE_SIZE;
    this.positionsArrayOffset = valueSerializerOffset + OByteSerializer.BYTE_SIZE;

    this.sysMagicOffset = endCreatedBuckets;
    this.freeSpaceOffset = sysMagicOffset + OByteSerializer.BYTE_SIZE;
    this.freeListHeadOffset = freeSpaceOffset + 2 * OIntegerSerializer.INT_SIZE;
    this.freeListLengthOffset = freeListHeadOffset + 2 * OIntegerSerializer.INT_SIZE;
  }

  OBonsaiBucketAbstractV2(OCacheEntry cacheEntry, int pageOffset, int maxBucketSizeInBytes) {
    super(cacheEntry);

    this.maxBucketSizeInBytes = maxBucketSizeInBytes;
    this.amountOfBuckets = PAGE_SIZE / maxBucketSizeInBytes;
    this.bucketsFillSize = (amountOfBuckets + BITS_IN_BYTE - 1) / BITS_IN_BYTE;
    this.endCreatedBuckets = CREATED_BUCKETS_OFFSET + bucketsFillSize;

    this.freePointerOffset = endCreatedBuckets;
    this.sizeOffset = freePointerOffset + OIntegerSerializer.INT_SIZE;
    this.flagsOffset = sizeOffset + OIntegerSerializer.INT_SIZE;
    this.freeListPointerOffset = flagsOffset + OByteSerializer.BYTE_SIZE;
    this.leftSiblingOffset = freeListPointerOffset + 2 * OIntegerSerializer.INT_SIZE;
    this.rightSiblingOffset = leftSiblingOffset + 2 * OIntegerSerializer.INT_SIZE;
    this.treeSizeOffset = rightSiblingOffset + 2 * OIntegerSerializer.INT_SIZE;
    this.keySerializerOffset = treeSizeOffset + OLongSerializer.LONG_SIZE;
    this.valueSerializerOffset = keySerializerOffset + OByteSerializer.BYTE_SIZE;
    this.positionsArrayOffset = valueSerializerOffset + OByteSerializer.BYTE_SIZE;

    this.sysMagicOffset = endCreatedBuckets;
    this.freeSpaceOffset = sysMagicOffset + OByteSerializer.BYTE_SIZE;
    this.freeListHeadOffset = freeSpaceOffset + 2 * OIntegerSerializer.INT_SIZE;
    this.freeListLengthOffset = freeListHeadOffset + 2 * OIntegerSerializer.INT_SIZE;

    final int bucketIndex = pageOffset / maxBucketSizeInBytes;
    final int byteIndex = bucketIndex / BITS_IN_BYTE;
    final int bitIndex = bucketIndex - byteIndex * BITS_IN_BYTE;

    byte fillByte = buffer.get(CREATED_BUCKETS_OFFSET + byteIndex);
    fillByte = (byte) ((1 << bitIndex) | fillByte);

    buffer.put(CREATED_BUCKETS_OFFSET + byteIndex, fillByte);
  }

  /**
   * Write a bucket pointer to specific location.
   *
   * @param pageOffset where to write
   * @param value      - pointer to write
   */
  final void setBucketPointer(int pageOffset, OBonsaiBucketPointer value) {
    buffer.putInt(pageOffset, value.getPageIndex());
    buffer.putInt(pageOffset + OIntegerSerializer.INT_SIZE, value.getPageOffset());
  }

  /**
   * Read bucket pointer from page.
   *
   * @param offset where the pointer should be read from
   *
   * @return bucket pointer
   */
  final OBonsaiBucketPointer getBucketPointer(int offset) {
    final int pageIndex = buffer.getInt(offset);
    final int pageOffset = buffer.getInt(offset + OIntegerSerializer.INT_SIZE);
    return new OBonsaiBucketPointer(pageIndex, pageOffset, OSBTreeBonsaiLocalV2.BINARY_VERSION);
  }

  final void setDeleted(final int offset, @SuppressWarnings("SameParameterValue") final byte deletionFlag) {
    final byte value = buffer.get(offset + flagsOffset);
    buffer.put(offset + flagsOffset, (byte) (value | deletionFlag));

    final int bucketIndex = offset / maxBucketSizeInBytes;
    final int byteIndex = bucketIndex / BITS_IN_BYTE;
    final int bitIndex = bucketIndex - byteIndex * BITS_IN_BYTE;

    byte fillByte = buffer.get(CREATED_BUCKETS_OFFSET + byteIndex);
    fillByte = (byte) (~(1 << bitIndex) & fillByte);

    buffer.put(CREATED_BUCKETS_OFFSET + byteIndex, fillByte);
  }

  boolean isDeleted(final int offset, @SuppressWarnings("SameParameterValue") final byte deletionFlag) {
    return (buffer.get(offset + flagsOffset) & deletionFlag) == deletionFlag;
  }

  @Override
  public int serializedSize() {
    if (cacheEntry.getPageIndex() == OSBTreeBonsaiLocalV2.SYS_BUCKET.getPageIndex()) {

      final int sysDataEnd = freeListLengthOffset + OLongSerializer.LONG_SIZE;
      int size = serializedBucketsSize();
      size += sysDataEnd + OByteSerializer.BYTE_SIZE;

      return size;
    } else {
      return serializedBucketsSize() + OByteSerializer.BYTE_SIZE;
    }
  }

  @Override
  public final void serializePage(ByteBuffer recordBuffer) {
    assert buffer.limit() == buffer.capacity();

    if (cacheEntry.getPageIndex() == OSBTreeBonsaiLocalV2.SYS_BUCKET.getPageIndex()) {
      final int sysDataEnd = freeListLengthOffset + OLongSerializer.LONG_SIZE;

      recordBuffer.put((byte) 0);

      this.buffer.position(OByteSerializer.BYTE_SIZE);
      this.buffer.limit(sysDataEnd + OByteSerializer.BYTE_SIZE);
      recordBuffer.put(this.buffer);
      buffer.limit(buffer.capacity());

      serializeBuckets(recordBuffer);
    } else {
      recordBuffer.put((byte) 1);

      serializeBuckets(recordBuffer);
    }
  }

  @Override
  public final void deserializePage(byte[] page) {
    assert buffer.limit() == buffer.capacity();

    if (page[0] == 0) {
      final int sysDataEnd = freeListLengthOffset + OLongSerializer.LONG_SIZE;

      buffer.position(0);
      buffer.put(page, 1, sysDataEnd);

      deserializeBuckets(page, sysDataEnd + 1);
    } else if (page[0] == 1) {
      deserializeBuckets(page, 1);
    } else {
      throw new IllegalStateException("Illegal page type " + page[0]);
    }
  }

  @Override
  protected PageSerializationType serializationType() {
    return PageSerializationType.SBTREE_BONSAI_BUCKET;
  }

  private int serializedBucketsSize() {
    int size = bucketsFillSize;

    int bitCounter = 0;
    int byteCounter = 0;

    final byte[] fillBytes = new byte[amountOfBuckets];
    buffer.position(CREATED_BUCKETS_OFFSET);
    buffer.get(fillBytes);

    byte fillByte = fillBytes[byteCounter];

    for (int i = 0; i < amountOfBuckets; i++, bitCounter++) {
      if (bitCounter >= BITS_IN_BYTE) {
        byteCounter++;
        bitCounter = 0;
        fillByte = fillBytes[byteCounter];
      }

      if (fillByte != 0) {
        final int bitMask = 1 << bitCounter;

        if ((fillByte & bitMask) != 0) {
          final int offset = i * maxBucketSizeInBytes;
          final int bucketSize = buffer.getInt(offset + sizeOffset);
          final int positionsSize = bucketSize * OIntegerSerializer.INT_SIZE;

          size += positionsArrayOffset + positionsSize;
          final int freePointer = buffer.getInt(offset + freePointerOffset);
          final int dataSize = maxBucketSizeInBytes - freePointer;
          size += dataSize;

          fillByte = (byte) (fillByte & (~bitMask));
        }

      } else {
        i += BITS_IN_BYTE - bitCounter - 1;
        assert (i & (BITS_IN_BYTE - 1)) == 7;

        bitCounter = BITS_IN_BYTE;
      }
    }

    return size;
  }

  private void serializeBuckets(final ByteBuffer recordBuffer) {
    final byte[] fillBytes = new byte[bucketsFillSize];

    buffer.position(CREATED_BUCKETS_OFFSET);
    buffer.get(fillBytes);
    recordBuffer.put(fillBytes);

    int bitCounter = 0;
    int byteCounter = 0;

    byte fillByte = fillBytes[byteCounter];

    for (int i = 0; i < amountOfBuckets; i++, bitCounter++) {
      if (bitCounter >= BITS_IN_BYTE) {
        byteCounter++;
        bitCounter = 0;
        fillByte = fillBytes[byteCounter];
      }

      if (fillByte != 0) {
        final int bitMask = 1 << bitCounter;

        if ((fillByte & bitMask) != 0) {
          final int offset = i * maxBucketSizeInBytes;
          final int bucketSize = buffer.getInt(offset + sizeOffset);
          final int positionsSize = bucketSize * OIntegerSerializer.INT_SIZE;

          final int headerSize = positionsArrayOffset + positionsSize;

          buffer.position(offset);
          buffer.limit(offset + headerSize);
          recordBuffer.put(buffer);
          buffer.limit(buffer.capacity());

          final int freePointer = buffer.getInt(offset + freePointerOffset);
          final int dataSize = maxBucketSizeInBytes - freePointer;

          if (dataSize > 0) {
            buffer.position(offset + freePointer);
            buffer.limit(offset + freePointer + dataSize);
            recordBuffer.put(buffer);
            buffer.limit(buffer.capacity());
          }

          fillByte = (byte) (fillByte & (~bitMask));
        }
      } else {
        i += BITS_IN_BYTE - bitCounter - 1;
        assert (i & (BITS_IN_BYTE - 1)) == 7;

        bitCounter = BITS_IN_BYTE;
      }
    }
  }

  private void deserializeBuckets(final byte[] content, int dataOffset) {
    final int startOffset = dataOffset;

    buffer.position(CREATED_BUCKETS_OFFSET);
    buffer.put(content, dataOffset, bucketsFillSize);

    dataOffset += bucketsFillSize;

    int bitCounter = 0;
    int byteCounter = 0;

    byte fillByte = content[startOffset + byteCounter];

    for (int i = 0; i < amountOfBuckets; i++, bitCounter++) {
      if (bitCounter >= BITS_IN_BYTE) {
        byteCounter++;
        bitCounter = 0;

        fillByte = content[startOffset + byteCounter];
      }

      if (fillByte != 0) {
        if ((fillByte & (1 << bitCounter)) != 0) {
          final int offset = i * maxBucketSizeInBytes;
          buffer.position(offset);
          buffer.put(content, dataOffset, positionsArrayOffset);
          dataOffset += positionsArrayOffset;

          final int bucketSize = buffer.getInt(offset + sizeOffset);
          final int positionsSize = bucketSize * OIntegerSerializer.INT_SIZE;

          if (positionsSize > 0) {
            buffer.position(offset + positionsArrayOffset);
            buffer.put(content, dataOffset, positionsSize);
            dataOffset += positionsSize;

            final int freePointer = buffer.getInt(offset + freePointerOffset);
            final int dataSize = maxBucketSizeInBytes - freePointer;

            buffer.position(freePointer + offset);
            buffer.put(content, dataOffset, dataSize);
            dataOffset += dataSize;
          }
        }
      } else {
        i += BITS_IN_BYTE - bitCounter - 1;
        assert (i & (BITS_IN_BYTE - 1)) == 7;

        bitCounter = BITS_IN_BYTE;
      }
    }
  }

}
