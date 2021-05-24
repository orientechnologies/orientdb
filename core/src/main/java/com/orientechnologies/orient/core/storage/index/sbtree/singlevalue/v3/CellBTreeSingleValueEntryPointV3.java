package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.entrypoint.CellBTreeEntryPointSingleValueV3InitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.entrypoint.CellBTreeEntryPointSingleValueV3SetPagesSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.entrypoint.CellBTreeEntryPointSingleValueV3SetTreeSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.entrypoint.CellBTreeSingleValueEntryPointV3SetFreeListHeadPO;

public final class CellBTreeSingleValueEntryPointV3<K> extends ODurablePage {
  private static final int KEY_SERIALIZER_OFFSET = NEXT_FREE_POSITION;
  private static final int KEY_SIZE_OFFSET = KEY_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int TREE_SIZE_OFFSET = KEY_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int PAGES_SIZE_OFFSET = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int FREE_LIST_HEAD_OFFSET = PAGES_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  public CellBTreeSingleValueEntryPointV3(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init() {
    setLongValue(TREE_SIZE_OFFSET, 0);
    setIntValue(PAGES_SIZE_OFFSET, 1);
    setIntValue(FREE_LIST_HEAD_OFFSET, -1);

    addPageOperation(new CellBTreeEntryPointSingleValueV3InitPO());
  }

  public void setTreeSize(final long size) {
    final long prevSize = getIntValue(TREE_SIZE_OFFSET);

    setLongValue(TREE_SIZE_OFFSET, size);

    addPageOperation(new CellBTreeEntryPointSingleValueV3SetTreeSizePO(prevSize, size));
  }

  public long getTreeSize() {
    return getLongValue(TREE_SIZE_OFFSET);
  }

  public void setPagesSize(final int pages) {
    final int prevPagesSize = getIntValue(PAGES_SIZE_OFFSET);

    setIntValue(PAGES_SIZE_OFFSET, pages);
    addPageOperation(new CellBTreeEntryPointSingleValueV3SetPagesSizePO(prevPagesSize, pages));
  }

  public int getPagesSize() {
    return getIntValue(PAGES_SIZE_OFFSET);
  }

  public int getFreeListHead() {
    return getIntValue(FREE_LIST_HEAD_OFFSET);
  }

  public void setFreeListHead(int freeListHead) {
    final int prevFreeListHead = getIntValue(FREE_LIST_HEAD_OFFSET);

    setIntValue(FREE_LIST_HEAD_OFFSET, freeListHead);
    addPageOperation(
        new CellBTreeSingleValueEntryPointV3SetFreeListHeadPO(freeListHead, prevFreeListHead));
  }
}
