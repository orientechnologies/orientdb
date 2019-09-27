package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.entrypoint;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2.CellBTreeMultiValueV2EntryPoint;

public final class CellBTreeMultiValueV2EntryPointInitPO extends PageOperationRecord {
  @Override
  public void redo(final OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV2EntryPoint bucket = new CellBTreeMultiValueV2EntryPoint(cacheEntry);
    bucket.init();
  }

  @Override
  public void undo(final OCacheEntry cacheEntry) {
    //ignore
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_ENTRY_POINT_MULTI_VALUE_V2_INIT_PO;
  }
}
