/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.index.hashindex.local;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;

import java.io.IOException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 5/14/14
 */
public class ODirectoryFirstPage extends ODirectoryPage {
  private static final int TREE_SIZE_OFFSET = NEXT_FREE_POSITION;
  private static final int TOMBSTONE_OFFSET = TREE_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int ITEMS_OFFSET     = TOMBSTONE_OFFSET + OIntegerSerializer.INT_SIZE;

  public static final int  NODES_PER_PAGE   = (OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024 - ITEMS_OFFSET)
                                                / OHashTableDirectory.BINARY_LEVEL_SIZE;

  public ODirectoryFirstPage(OCacheEntry cacheEntry, OWALChangesTree changesTree, OCacheEntry entry) {
    super(cacheEntry, changesTree, entry);
  }

  public void setTreeSize(int treeSize) throws IOException {
    setIntValue(TREE_SIZE_OFFSET, treeSize);
  }

  public int getTreeSize() {
    return getIntValue(TREE_SIZE_OFFSET);
  }

  public void setTombstone(int tombstone) throws IOException {
    setIntValue(TOMBSTONE_OFFSET, tombstone);
  }

  public int getTombstone() {
    return getIntValue(TOMBSTONE_OFFSET);
  }

  @Override
  protected int getItemsOffset() {
    return ITEMS_OFFSET;
  }
}
