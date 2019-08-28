package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.entrypoint.CellBTreeEntryPointSingleValueV1InitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.entrypoint.CellBTreeEntryPointSingleValueV1SetPagesSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.entrypoint.CellBTreeEntryPointSingleValueV1SetTreeSizePO;

public final class CellBTreeSingleValueEntryPointV1 extends ODurablePage {
  private static final int KEY_SERIALIZER_OFFSET = NEXT_FREE_POSITION;
  private static final int KEY_SIZE_OFFSET       = KEY_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int TREE_SIZE_OFFSET      = KEY_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int PAGES_SIZE_OFFSET     = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;

  public CellBTreeSingleValueEntryPointV1(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init() {
    setLongValue(TREE_SIZE_OFFSET, 0);
    setIntValue(PAGES_SIZE_OFFSET, 1);

    addPageOperation(new CellBTreeEntryPointSingleValueV1InitPO());
  }

  public void setTreeSize(final long size) {
    final long prevSize = getLongValue(TREE_SIZE_OFFSET);

    setLongValue(TREE_SIZE_OFFSET, size);
    addPageOperation(new CellBTreeEntryPointSingleValueV1SetTreeSizePO(prevSize, size));
  }

  public long getTreeSize() {
    return getLongValue(TREE_SIZE_OFFSET);
  }

  public void setPagesSize(final int pages) {
    final int prevPages = getIntValue(PAGES_SIZE_OFFSET);

    setIntValue(PAGES_SIZE_OFFSET, pages);
    addPageOperation(new CellBTreeEntryPointSingleValueV1SetPagesSizePO(prevPages, pages));
  }

  public int getPagesSize() {
    return getIntValue(PAGES_SIZE_OFFSET);
  }
}
