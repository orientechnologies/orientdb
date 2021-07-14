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

package com.orientechnologies.orient.core.storage.index.hashindex.local.v2;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/14/14
 */
public class DirectoryPageV2 extends ODurablePage {
  private static final int ITEMS_OFFSET = NEXT_FREE_POSITION;

  static final int NODES_PER_PAGE =
      (OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024 - ITEMS_OFFSET)
          / HashTableDirectory.BINARY_LEVEL_SIZE;

  public DirectoryPageV2(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void setMaxLeftChildDepth(int localNodeIndex, byte maxLeftChildDepth) {
    final int offset = getItemsOffset() + localNodeIndex * HashTableDirectory.BINARY_LEVEL_SIZE;
    setByteValue(offset, maxLeftChildDepth);
  }

  public byte getMaxLeftChildDepth(int localNodeIndex) {
    int offset = getItemsOffset() + localNodeIndex * HashTableDirectory.BINARY_LEVEL_SIZE;
    return getByteValue(offset);
  }

  public void setMaxRightChildDepth(int localNodeIndex, byte maxRightChildDepth) {
    final int offset =
        getItemsOffset()
            + localNodeIndex * HashTableDirectory.BINARY_LEVEL_SIZE
            + OByteSerializer.BYTE_SIZE;
    setByteValue(offset, maxRightChildDepth);
  }

  public byte getMaxRightChildDepth(int localNodeIndex) {
    int offset =
        getItemsOffset()
            + localNodeIndex * HashTableDirectory.BINARY_LEVEL_SIZE
            + OByteSerializer.BYTE_SIZE;
    return getByteValue(offset);
  }

  public void setNodeLocalDepth(int localNodeIndex, byte nodeLocalDepth) {
    final int offset =
        getItemsOffset()
            + localNodeIndex * HashTableDirectory.BINARY_LEVEL_SIZE
            + 2 * OByteSerializer.BYTE_SIZE;

    setByteValue(offset, nodeLocalDepth);
  }

  public byte getNodeLocalDepth(int localNodeIndex) {
    int offset =
        getItemsOffset()
            + localNodeIndex * HashTableDirectory.BINARY_LEVEL_SIZE
            + 2 * OByteSerializer.BYTE_SIZE;
    return getByteValue(offset);
  }

  public void setPointer(int localNodeIndex, int index, long pointer) {
    final int offset =
        getItemsOffset()
            + (localNodeIndex * HashTableDirectory.BINARY_LEVEL_SIZE
                + 3 * OByteSerializer.BYTE_SIZE)
            + index * HashTableDirectory.ITEM_SIZE;

    setLongValue(offset, pointer);
  }

  public long getPointer(int localNodeIndex, int index) {
    int offset =
        getItemsOffset()
            + (localNodeIndex * HashTableDirectory.BINARY_LEVEL_SIZE
                + 3 * OByteSerializer.BYTE_SIZE)
            + index * HashTableDirectory.ITEM_SIZE;

    return getLongValue(offset);
  }

  protected int getItemsOffset() {
    return ITEMS_OFFSET;
  }
}
