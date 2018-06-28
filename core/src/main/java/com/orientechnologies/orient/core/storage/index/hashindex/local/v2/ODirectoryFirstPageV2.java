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

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/14/14
 */
public final class ODirectoryFirstPageV2 extends ODirectoryPageV2 {
  private static final int TREE_SIZE_OFFSET = NEXT_FREE_POSITION;
  private static final int TOMBSTONE_OFFSET = TREE_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int ITEMS_OFFSET = TOMBSTONE_OFFSET + OIntegerSerializer.INT_SIZE;

  static final int NODES_PER_PAGE = (OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024 - ITEMS_OFFSET)
      / OHashTableDirectoryV2.BINARY_LEVEL_SIZE;

  ODirectoryFirstPageV2(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  void setTreeSize(final int treeSize) {
    buffer.putInt(TREE_SIZE_OFFSET, treeSize);
  }

  int getTreeSize() {
    return buffer.getInt(TREE_SIZE_OFFSET);
  }

  void setTombstone(final int tombstone) {
    buffer.putInt(TOMBSTONE_OFFSET, tombstone);
  }

  int getTombstone() {
    return buffer.getInt(TOMBSTONE_OFFSET);
  }

  @Override
  protected int getItemsOffset() {
    return ITEMS_OFFSET;
  }
}
