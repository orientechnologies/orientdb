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

package com.orientechnologies.orient.core.storage.index.hashindex.local;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/14/14
 */
public class ODirectoryPage extends ODurablePage {
  private static final int ITEMS_OFFSET = NEXT_FREE_POSITION;

  static final int NODES_PER_PAGE =
      (OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024 - ITEMS_OFFSET) / OHashTableDirectory.BINARY_LEVEL_SIZE;

  private final OCacheEntry entry;

  ODirectoryPage(final OCacheEntry cacheEntry, final OCacheEntry entry) {
    super(cacheEntry);
    this.entry = entry;
  }

  public OCacheEntry getEntry() {
    return entry;
  }

  void setMaxLeftChildDepth(final int localNodeIndex, final byte maxLeftChildDepth) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectory.BINARY_LEVEL_SIZE;
    buffer.put(offset, maxLeftChildDepth);
    cacheEntry.markDirty();
  }

  byte getMaxLeftChildDepth(final int localNodeIndex) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectory.BINARY_LEVEL_SIZE;
    return buffer.get(offset);
  }

  void setMaxRightChildDepth(final int localNodeIndex, final byte maxRightChildDepth) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectory.BINARY_LEVEL_SIZE + OByteSerializer.BYTE_SIZE;
    buffer.put(offset, maxRightChildDepth);
    cacheEntry.markDirty();
  }

  byte getMaxRightChildDepth(final int localNodeIndex) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectory.BINARY_LEVEL_SIZE + OByteSerializer.BYTE_SIZE;
    return buffer.get(offset);
  }

  void setNodeLocalDepth(final int localNodeIndex, final byte nodeLocalDepth) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectory.BINARY_LEVEL_SIZE + 2 * OByteSerializer.BYTE_SIZE;
    buffer.put(offset, nodeLocalDepth);
    cacheEntry.markDirty();
  }

  byte getNodeLocalDepth(final int localNodeIndex) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectory.BINARY_LEVEL_SIZE + 2 * OByteSerializer.BYTE_SIZE;
    return buffer.get(offset);
  }

  void setPointer(final int localNodeIndex, final int index, final long pointer) {
    final int offset = getItemsOffset() + (localNodeIndex * OHashTableDirectory.BINARY_LEVEL_SIZE + 3 * OByteSerializer.BYTE_SIZE)
        + index * OHashTableDirectory.ITEM_SIZE;

    buffer.putLong(offset, pointer);
    cacheEntry.markDirty();
  }

  public long getPointer(final int localNodeIndex, final int index) {
    final int offset = getItemsOffset() + (localNodeIndex * OHashTableDirectory.BINARY_LEVEL_SIZE + 3 * OByteSerializer.BYTE_SIZE)
        + index * OHashTableDirectory.ITEM_SIZE;

    return buffer.getLong(offset);
  }

  protected int getItemsOffset() {
    return ITEMS_OFFSET;
  }
}
