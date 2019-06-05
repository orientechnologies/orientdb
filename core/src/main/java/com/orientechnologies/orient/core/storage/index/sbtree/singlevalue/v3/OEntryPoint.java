package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

final class OEntryPoint<K> extends ODurablePage {
  private static final int KEY_SERIALIZER_OFFSET = NEXT_FREE_POSITION;
  private static final int KEY_SIZE_OFFSET       = KEY_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int TREE_SIZE_OFFSET      = KEY_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int PAGES_SIZE_OFFSET     = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;

  OEntryPoint(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  void init() {
    setLongValue(TREE_SIZE_OFFSET, 0);
    setIntValue(PAGES_SIZE_OFFSET, 1);
  }

  void setTreeSize(final long size) {
    setLongValue(TREE_SIZE_OFFSET, size);
  }

  long getTreeSize() {
    return getLongValue(TREE_SIZE_OFFSET);
  }

  void setPagesSize(final int pages) {
    setIntValue(PAGES_SIZE_OFFSET, pages);
  }

  int getPagesSize() {
    return getIntValue(PAGES_SIZE_OFFSET);
  }
}
