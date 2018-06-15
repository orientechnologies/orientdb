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
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;

/**
 * A base class for bonsai buckets. Bonsai bucket size is usually less than page size and one page could contain multiple bonsai
 * buckets.
 * Adds methods to read and write bucket pointers.
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 * @see OBonsaiBucketPointer
 * @see com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsai
 */
class OBonsaiBucketAbstractV2 extends ODurablePage {
  private static final int BITS_IN_BYTE          = 8;
  static final         int MAX_BUCKET_SIZE_BYTES = OGlobalConfiguration.SBTREEBONSAI_BUCKET_SIZE.getValueAsInteger() * 1024;

  private static final int CREATED_BUCKETS_OFFSET = NEXT_FREE_POSITION;
  private static final int AMOUNT_OF_BUCKETS      = MAX_PAGE_SIZE_BYTES / MAX_BUCKET_SIZE_BYTES;
  private static final int BUCKETS_FILL_SIZE      = (AMOUNT_OF_BUCKETS + BITS_IN_BYTE - 1) / BITS_IN_BYTE;

  private static final int END_CREATED_BUCKETS = CREATED_BUCKETS_OFFSET + BUCKETS_FILL_SIZE;

  static final int FREE_POINTER_OFFSET      = END_CREATED_BUCKETS;
  static final int SIZE_OFFSET              = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  static final int FLAGS_OFFSET             = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  static final int FREE_LIST_POINTER_OFFSET = FLAGS_OFFSET + OByteSerializer.BYTE_SIZE;
  static final int LEFT_SIBLING_OFFSET      = FREE_LIST_POINTER_OFFSET + 2 * OIntegerSerializer.INT_SIZE;
  static final int RIGHT_SIBLING_OFFSET     = LEFT_SIBLING_OFFSET + 2 * OIntegerSerializer.INT_SIZE;
  static final int TREE_SIZE_OFFSET         = RIGHT_SIBLING_OFFSET + 2 * OIntegerSerializer.INT_SIZE;
  static final int KEY_SERIALIZER_OFFSET    = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;
  static final int VALUE_SERIALIZER_OFFSET  = KEY_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;
  static final int POSITIONS_ARRAY_OFFSET   = VALUE_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;

  static final int SYS_MAGIC_OFFSET        = END_CREATED_BUCKETS;
  static final int FREE_SPACE_OFFSET       = SYS_MAGIC_OFFSET + OByteSerializer.BYTE_SIZE;
  static final int FREE_LIST_HEAD_OFFSET   = FREE_SPACE_OFFSET + 2 * OIntegerSerializer.INT_SIZE;
  static final int FREE_LIST_LENGTH_OFFSET = FREE_LIST_HEAD_OFFSET + 2 * OIntegerSerializer.INT_SIZE;

  OBonsaiBucketAbstractV2(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  OBonsaiBucketAbstractV2(OCacheEntry cacheEntry, int pageOffset) {
    super(cacheEntry);

    final int bucketIndex = pageOffset / MAX_BUCKET_SIZE_BYTES;
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

    cacheEntry.markDirty();
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
    final byte value = buffer.get(offset + FLAGS_OFFSET);
    buffer.put(offset + FLAGS_OFFSET, (byte) (value | deletionFlag));

    final int bucketIndex = offset / MAX_BUCKET_SIZE_BYTES;
    final int byteIndex = bucketIndex / BITS_IN_BYTE;
    final int bitIndex = bucketIndex - byteIndex * BITS_IN_BYTE;

    byte fillByte = buffer.get(CREATED_BUCKETS_OFFSET + byteIndex);
    fillByte = (byte) (~(1 << bitIndex) & fillByte);

    buffer.put(CREATED_BUCKETS_OFFSET + byteIndex, fillByte);

    cacheEntry.markDirty();
  }

  boolean isDeleted(final int offset, @SuppressWarnings("SameParameterValue") final byte deletionFlag) {
    return (buffer.get(offset + FLAGS_OFFSET) & deletionFlag) == deletionFlag;
  }

  @Override
  protected final byte[] serializePage() {
    final byte[] page;

    if (cacheEntry.getPageIndex() == OSBTreeBonsaiLocalV2.SYS_BUCKET.getPageIndex()) {
      final int sysDataEnd = FREE_LIST_LENGTH_OFFSET + OLongSerializer.LONG_SIZE;
      int size = serializedBucketsSize();
      size += sysDataEnd + OByteSerializer.BYTE_SIZE;

      page = new byte[size];

      buffer.position(0);
      buffer.get(page, OByteSerializer.BYTE_SIZE, sysDataEnd);
      serializeBuckets(page, sysDataEnd + OByteSerializer.BYTE_SIZE);
    } else {
      final int size = serializedBucketsSize() + OByteSerializer.BYTE_SIZE;
      page = new byte[size];
      page[0] = 1;
      serializeBuckets(page, OByteSerializer.BYTE_SIZE);
    }

    return page;
  }

  @Override
  protected final void deserializePage(byte[] page) {
    if (page[0] == 0) {
      final int sysDataEnd = FREE_LIST_LENGTH_OFFSET + OLongSerializer.LONG_SIZE;

      buffer.position(0);
      buffer.put(page, 1, sysDataEnd);

      deserializeBuckets(page, sysDataEnd + 1);
    } else if (page[0] == 1) {
      deserializeBuckets(page, 1);
    } else {
      throw new IllegalStateException("Illegal page type " + page[0]);
    }

    cacheEntry.markDirty();
  }

  private int serializedBucketsSize() {
    int size = BUCKETS_FILL_SIZE;

    int bitCounter = 0;
    int byteCounter = 0;

    final byte[] fillBytes = new byte[AMOUNT_OF_BUCKETS];
    buffer.position(CREATED_BUCKETS_OFFSET);
    buffer.get(fillBytes);

    byte fillByte = fillBytes[byteCounter];

    for (int i = 0; i < AMOUNT_OF_BUCKETS; i++, bitCounter++) {
      if (bitCounter >= BITS_IN_BYTE) {
        byteCounter++;
        bitCounter = 0;
        fillByte = fillBytes[byteCounter];
      }

      if (fillByte != 0) {
        final int bitMask = 1 << bitCounter;

        if ((fillByte & bitMask) != 0) {
          final int offset = i * MAX_BUCKET_SIZE_BYTES;
          final int bucketSize = buffer.getInt(offset + SIZE_OFFSET);
          final int positionsSize = bucketSize * OIntegerSerializer.INT_SIZE;

          size += POSITIONS_ARRAY_OFFSET + positionsSize;
          final int freePointer = buffer.getInt(offset + FREE_POINTER_OFFSET);
          final int dataSize = MAX_BUCKET_SIZE_BYTES - freePointer;
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

  private void serializeBuckets(final byte[] content, int dataOffset) {
    final int startOffset = dataOffset;

    buffer.position(CREATED_BUCKETS_OFFSET);
    buffer.get(content, dataOffset, BUCKETS_FILL_SIZE);
    dataOffset += BUCKETS_FILL_SIZE;

    int bitCounter = 0;
    int byteCounter = 0;

    byte fillByte = content[startOffset + byteCounter];

    for (int i = 0; i < AMOUNT_OF_BUCKETS; i++, bitCounter++) {
      if (bitCounter >= BITS_IN_BYTE) {
        byteCounter++;
        bitCounter = 0;
        fillByte = content[startOffset + byteCounter];
      }

      if (fillByte != 0) {
        final int bitMask = 1 << bitCounter;

        if ((fillByte & bitMask) != 0) {
          final int offset = i * MAX_BUCKET_SIZE_BYTES;
          final int bucketSize = buffer.getInt(offset + SIZE_OFFSET);
          final int positionsSize = bucketSize * OIntegerSerializer.INT_SIZE;

          final int headerSize = POSITIONS_ARRAY_OFFSET + positionsSize;

          buffer.position(offset);
          buffer.get(content, dataOffset, headerSize);
          dataOffset += headerSize;

          final int freePointer = buffer.getInt(offset + FREE_POINTER_OFFSET);
          final int dataSize = MAX_BUCKET_SIZE_BYTES - freePointer;

          if (dataSize > 0) {
            buffer.position(offset + freePointer);
            buffer.get(content, dataOffset, dataSize);
            dataOffset += dataSize;
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
    buffer.put(content, dataOffset, BUCKETS_FILL_SIZE);

    dataOffset += BUCKETS_FILL_SIZE;

    int bitCounter = 0;
    int byteCounter = 0;

    byte fillByte = content[startOffset + byteCounter];

    for (int i = 0; i < AMOUNT_OF_BUCKETS; i++, bitCounter++) {
      if (bitCounter >= BITS_IN_BYTE) {
        byteCounter++;
        bitCounter = 0;

        fillByte = content[startOffset + byteCounter];
      }

      if (fillByte != 0) {
        if ((fillByte & (1 << bitCounter)) != 0) {
          final int offset = i * MAX_BUCKET_SIZE_BYTES;
          buffer.position(offset);
          buffer.put(content, dataOffset, POSITIONS_ARRAY_OFFSET);
          dataOffset += POSITIONS_ARRAY_OFFSET;

          final int bucketSize = buffer.getInt(offset + SIZE_OFFSET);
          final int positionsSize = bucketSize * OIntegerSerializer.INT_SIZE;

          if (positionsSize > 0) {
            buffer.position(offset + POSITIONS_ARRAY_OFFSET);
            buffer.put(content, dataOffset, positionsSize);
            dataOffset += positionsSize;

            final int freePointer = buffer.getInt(offset + FREE_POINTER_OFFSET);
            final int dataSize = MAX_BUCKET_SIZE_BYTES - freePointer;

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
