package com.orientechnologies.orient.core.storage.index.nkbtree.binarybtree;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

public final class EntryPoint extends ODurablePage {
  private static final int TREE_SIZE_OFFSET = NEXT_FREE_POSITION + OIntegerSerializer.INT_SIZE;
  private static final int PAGES_SIZE_OFFSET = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int FREE_LIST_HEAD_OFFSET = PAGES_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  public EntryPoint(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init() {
    setLongValue(TREE_SIZE_OFFSET, 0);
    setIntValue(PAGES_SIZE_OFFSET, 1);
    setIntValue(FREE_LIST_HEAD_OFFSET, -1);
  }

  public void setTreeSize(final long size) {
    setLongValue(TREE_SIZE_OFFSET, size);
  }

  public long getTreeSize() {
    return getLongValue(TREE_SIZE_OFFSET);
  }

  public void setPagesSize(final int pages) {
    setIntValue(PAGES_SIZE_OFFSET, pages);
  }

  public int getPagesSize() {
    return getIntValue(PAGES_SIZE_OFFSET);
  }

  public int getFreeListHead() {
    return getIntValue(FREE_LIST_HEAD_OFFSET);
  }

  public void setFreeListHead(int freeListHead) {
    setIntValue(FREE_LIST_HEAD_OFFSET, freeListHead);
  }
}
