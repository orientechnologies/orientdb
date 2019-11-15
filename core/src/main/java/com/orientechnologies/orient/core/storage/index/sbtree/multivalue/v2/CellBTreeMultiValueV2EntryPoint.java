package com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.entrypoint.CellBTreeMultiValueV2EntryPointInitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.entrypoint.CellBTreeMultiValueV2EntryPointSetEntryIdPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.entrypoint.CellBTreeMultiValueV2EntryPointSetPagesSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.entrypoint.CellBTreeMultiValueV2EntryPointSetTreeSizePO;

public final class CellBTreeMultiValueV2EntryPoint<K> extends ODurablePage {
  private static final int KEY_SERIALIZER_OFFSET = NEXT_FREE_POSITION;
  private static final int KEY_SIZE_OFFSET       = KEY_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int TREE_SIZE_OFFSET      = KEY_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int PAGES_SIZE_OFFSET     = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int ENTRY_ID_OFFSET       = PAGES_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  public CellBTreeMultiValueV2EntryPoint(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init() {
    setLongValue(TREE_SIZE_OFFSET, 0);
    setIntValue(PAGES_SIZE_OFFSET, 1);
    setLongValue(ENTRY_ID_OFFSET, 0);

    addPageOperation(new CellBTreeMultiValueV2EntryPointInitPO());
  }

  public void setTreeSize(final long size) {
    final long prevSize = getLongValue(TREE_SIZE_OFFSET);

    setLongValue(TREE_SIZE_OFFSET, size);

    addPageOperation(new CellBTreeMultiValueV2EntryPointSetTreeSizePO(size, prevSize));
  }

  public long getTreeSize() {
    return getLongValue(TREE_SIZE_OFFSET);
  }

  public void setPagesSize(final int pages) {
    final int prevPages = getIntValue(PAGES_SIZE_OFFSET);

    setIntValue(PAGES_SIZE_OFFSET, pages);

    addPageOperation(new CellBTreeMultiValueV2EntryPointSetPagesSizePO(pages, prevPages));
  }

  public int getPagesSize() {
    return getIntValue(PAGES_SIZE_OFFSET);
  }

  public void setEntryId(final long id) {
    final long prevEntryId = getLongValue(ENTRY_ID_OFFSET);

    setLongValue(ENTRY_ID_OFFSET, id);
    addPageOperation(new CellBTreeMultiValueV2EntryPointSetEntryIdPO(id, prevEntryId));
  }

  public long getEntryId() {
    return getLongValue(ENTRY_ID_OFFSET);
  }
}
