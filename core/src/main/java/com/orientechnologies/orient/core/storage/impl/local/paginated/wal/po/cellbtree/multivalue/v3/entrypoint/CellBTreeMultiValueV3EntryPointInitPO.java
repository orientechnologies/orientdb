package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v3.entrypoint;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v3.CellBTreeMultiValueV3EntryPoint;

public final class CellBTreeMultiValueV3EntryPointInitPO extends PageOperationRecord {
  @Override
  public void redo(final OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV3EntryPoint bucket = new CellBTreeMultiValueV3EntryPoint(cacheEntry);
    bucket.init();
  }

  @Override
  public void undo(final OCacheEntry cacheEntry) {
    //ignore
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_ENTRY_POINT_MULTI_VALUE_V3_INIT_PO;
  }
}
