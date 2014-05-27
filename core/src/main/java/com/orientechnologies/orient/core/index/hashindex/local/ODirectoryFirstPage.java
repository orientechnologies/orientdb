package com.orientechnologies.orient.core.index.hashindex.local;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;

import java.io.IOException;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 5/14/14
 */
public class ODirectoryFirstPage extends ODirectoryPage {
  private static final int TREE_SIZE_OFFSET = NEXT_FREE_POSITION;
  private static final int TOMBSTONE_OFFSET = TREE_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int ITEMS_OFFSET     = TOMBSTONE_OFFSET + OIntegerSerializer.INT_SIZE;

  public static final int  NODES_PER_PAGE   = (OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024 - ITEMS_OFFSET)
                                                / OHashTableDirectory.BINARY_LEVEL_SIZE;

  public ODirectoryFirstPage(ODirectMemoryPointer pagePointer, TrackMode trackMode, OCacheEntry entry) {
    super(pagePointer, trackMode, entry);
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
