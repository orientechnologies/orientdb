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

package com.orientechnologies.orient.core.storage.index.hashindex.local.v3;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.PageSerializationType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/14/14
 */
public class ODirectoryPageV3 extends ODurablePage {
  private static final int BITS_PER_BYTE = 8;

  private static final int NODES_FILLED_OFFSET = NEXT_FREE_POSITION;
  private static final int NODES_FILLED_SIZE   = 32;
  static final         int NODES_FILLED_END    = NODES_FILLED_OFFSET + NODES_FILLED_SIZE / BITS_PER_BYTE;

  static final int NODES_PER_PAGE = (OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024 - NODES_FILLED_END)
      / OHashTableDirectoryV3.BINARY_LEVEL_SIZE;

  public ODirectoryPageV3(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  void markNodeAsAllocated(final int nodeIndex) {
    final int byteIndex = nodeIndex / BITS_PER_BYTE;
    final int bitIndex = nodeIndex - byteIndex * BITS_PER_BYTE;

    final byte fillByte = (byte) (buffer.get(byteIndex + NODES_FILLED_OFFSET) | (1 << bitIndex));
    buffer.put(byteIndex + NODES_FILLED_OFFSET, fillByte);

    cacheEntry.markDirty();
  }

  void markNodeAsDeleted(final int nodeIndex) {
    final int byteIndex = nodeIndex / BITS_PER_BYTE;
    final int bitIndex = nodeIndex - byteIndex * BITS_PER_BYTE;

    final byte fillByte = (byte) (buffer.get(byteIndex + NODES_FILLED_OFFSET) & (~(1 << bitIndex)));
    buffer.put(byteIndex + NODES_FILLED_OFFSET, fillByte);

    cacheEntry.markDirty();
  }

  void setMaxLeftChildDepth(final int localNodeIndex, final byte maxLeftChildDepth) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectoryV3.BINARY_LEVEL_SIZE;
    buffer.put(offset, maxLeftChildDepth);
    cacheEntry.markDirty();
  }

  byte getMaxLeftChildDepth(final int localNodeIndex) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectoryV3.BINARY_LEVEL_SIZE;
    return buffer.get(offset);
  }

  void setMaxRightChildDepth(final int localNodeIndex, final byte maxRightChildDepth) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectoryV3.BINARY_LEVEL_SIZE + OByteSerializer.BYTE_SIZE;
    buffer.put(offset, maxRightChildDepth);
    cacheEntry.markDirty();
  }

  byte getMaxRightChildDepth(final int localNodeIndex) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectoryV3.BINARY_LEVEL_SIZE + OByteSerializer.BYTE_SIZE;
    return buffer.get(offset);
  }

  void setNodeLocalDepth(final int localNodeIndex, final byte nodeLocalDepth) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectoryV3.BINARY_LEVEL_SIZE + 2 * OByteSerializer.BYTE_SIZE;
    buffer.put(offset, nodeLocalDepth);
    cacheEntry.markDirty();
  }

  byte getNodeLocalDepth(final int localNodeIndex) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectoryV3.BINARY_LEVEL_SIZE + 2 * OByteSerializer.BYTE_SIZE;
    return buffer.get(offset);
  }

  void setPointer(final int localNodeIndex, final int index, final long pointer) {
    final int offset = getItemsOffset() + (localNodeIndex * OHashTableDirectoryV3.BINARY_LEVEL_SIZE + 3 * OByteSerializer.BYTE_SIZE)
        + index * OHashTableDirectoryV3.ITEM_SIZE;

    buffer.putLong(offset, pointer);
    cacheEntry.markDirty();
  }

  public long getPointer(final int localNodeIndex, final int index) {
    final int offset = getItemsOffset() + (localNodeIndex * OHashTableDirectoryV3.BINARY_LEVEL_SIZE + 3 * OByteSerializer.BYTE_SIZE)
        + index * OHashTableDirectoryV3.ITEM_SIZE;

    return buffer.getLong(offset);
  }

  protected int getItemsOffset() {
    return NODES_FILLED_END;
  }

  final int serializedNodesSize() {
    return Integer.bitCount(buffer.getInt(NODES_FILLED_OFFSET)) * OHashTableDirectoryV3.BINARY_LEVEL_SIZE;
  }

  final void serializeNodes(final ByteBuffer recordBuffer) {
    final int nodesStart = getItemsOffset();

    int fillFlags = buffer.getInt(NODES_FILLED_OFFSET);

    for (int i = 0; i < NODES_FILLED_SIZE; i++) {
      final int mask = 1 << i;
      if ((fillFlags & mask) != 0) {
        final int nodeOffset = nodesStart + i * OHashTableDirectoryV3.BINARY_LEVEL_SIZE;

        buffer.position(nodeOffset);
        buffer.limit(nodeOffset + OHashTableDirectoryV3.BINARY_LEVEL_SIZE);
        recordBuffer.put(buffer);
        buffer.limit(buffer.capacity());
      }

      fillFlags = fillFlags & (~mask);
      if (fillFlags == 0) {
        break;
      }
    }
  }

  final void deserializeNodes(final byte[] page, int offset) {
    final int nodesStart = getItemsOffset();

    int fillFlags = buffer.get(NODES_FILLED_OFFSET);

    for (int i = 0; i < NODES_FILLED_SIZE; i++) {
      final int mask = 1 << i;
      if ((fillFlags & mask) != 0) {
        final int nodeOffset = nodesStart + i * OHashTableDirectoryV3.BINARY_LEVEL_SIZE;

        buffer.position(nodeOffset);
        buffer.put(page, offset, OHashTableDirectoryV3.BINARY_LEVEL_SIZE);

        offset += OHashTableDirectoryV3.BINARY_LEVEL_SIZE;
      }

      fillFlags = fillFlags & (~mask);
      if (fillFlags == 0) {
        break;
      }
    }
  }

  @Override
  public int serializedSize() {
    int size = NODES_FILLED_END;
    size += serializedNodesSize();

    return size;
  }

  @Override
  public void serializePage(ByteBuffer recordBuffer) {
    assert buffer.limit() == buffer.capacity();

    this.buffer.position(0);
    this.buffer.limit(NODES_FILLED_END);
    recordBuffer.put(this.buffer);
    this.buffer.limit(this.buffer.capacity());

    serializeNodes(recordBuffer);
  }

  @Override
  public void deserializePage(byte[] page) {
    assert buffer.limit() == buffer.capacity();

    buffer.position(0);
    buffer.put(page, 0, NODES_FILLED_END);

    deserializeNodes(page, NODES_FILLED_END);

    cacheEntry.markDirty();
  }

  @Override
  protected PageSerializationType serializationType() {
    return PageSerializationType.HASH_DIRECTORY_PAGE;
  }
}
