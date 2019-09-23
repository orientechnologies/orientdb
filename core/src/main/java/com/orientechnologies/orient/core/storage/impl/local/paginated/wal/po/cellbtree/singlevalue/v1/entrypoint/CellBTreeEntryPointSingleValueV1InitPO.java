package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.entrypoint;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1.CellBTreeSingleValueEntryPointV1;

public final class CellBTreeEntryPointSingleValueV1InitPO extends PageOperationRecord {
  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeSingleValueEntryPointV1 bucket = new CellBTreeSingleValueEntryPointV1(cacheEntry);
    bucket.init();
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V1_INIT_PO;
  }
}
