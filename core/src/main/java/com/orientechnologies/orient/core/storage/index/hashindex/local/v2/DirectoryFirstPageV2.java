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

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directoryfirstpage.*;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/14/14
 */
public final class DirectoryFirstPageV2 extends DirectoryPageV2 {
  private static final int TREE_SIZE_OFFSET = NEXT_FREE_POSITION;
  private static final int TOMBSTONE_OFFSET = TREE_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int ITEMS_OFFSET = TOMBSTONE_OFFSET + OIntegerSerializer.INT_SIZE;

  static final int NODES_PER_PAGE =
      (OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024 - ITEMS_OFFSET) / HashTableDirectory.BINARY_LEVEL_SIZE;

  public DirectoryFirstPageV2(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void setTreeSize(int treeSize) {
    final int pastSize = getIntValue(TREE_SIZE_OFFSET);
    setIntValue(TREE_SIZE_OFFSET, treeSize);

    addPageOperation(new LocalHashTableV2DirectoryFirstPageSetTreeSizePO(treeSize, pastSize));
  }

  public int getTreeSize() {
    return getIntValue(TREE_SIZE_OFFSET);
  }

  public void setTombstone(int tombstone) {
    final int pastTombstone = getIntValue(TOMBSTONE_OFFSET);
    setIntValue(TOMBSTONE_OFFSET, tombstone);
    addPageOperation(new LocalHashTableV2DirectoryFirstPageSetTombstonePO(tombstone, pastTombstone));
  }

  public int getTombstone() {
    return getIntValue(TOMBSTONE_OFFSET);
  }

  @Override
  protected int getItemsOffset() {
    return ITEMS_OFFSET;
  }

  @Override
  protected void logSetMaxLeftChildDepth(int localNodeIndex, byte maxLeftChildDepth, byte pastDepth) {
    addPageOperation(new LocalHashTableV2DirectoryFirstPageSetMaxLeftChildDepthPO(localNodeIndex, maxLeftChildDepth, pastDepth));
  }

  @Override
  protected void logSetMaxRightChildDepth(int localNodeIndex, byte maxRightChildDepth, byte pastDepth) {
    addPageOperation(new LocalHashTableV2DirectoryFirstPageSetMaxRightChildDepthPO(localNodeIndex, maxRightChildDepth, pastDepth));
  }

  @Override
  protected void logSetNodeLocalDepth(int localNodeIndex, byte nodeLocalDepth, byte pastDepth) {
    addPageOperation(new LocalHashTableV2DirectoryFirstPageSetNodeLocalDepthPO(localNodeIndex, nodeLocalDepth, pastDepth));
  }

  @Override
  protected void logSetPointer(int localNodeIndex, int index, long pointer, long pastPointer) {
    addPageOperation(new LocalHashTableV2DirectoryFirstPageSetPointerPO(localNodeIndex, index, pointer, pastPointer));
  }
}
