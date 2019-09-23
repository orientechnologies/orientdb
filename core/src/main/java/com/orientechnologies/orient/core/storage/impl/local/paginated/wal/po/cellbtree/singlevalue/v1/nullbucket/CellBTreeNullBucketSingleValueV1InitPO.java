package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.nullbucket;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1.CellBTreeNullBucketSingleValueV1;

public final class CellBTreeNullBucketSingleValueV1InitPO extends PageOperationRecord {
  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeNullBucketSingleValueV1 bucket = new CellBTreeNullBucketSingleValueV1(cacheEntry);
    bucket.init();
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V1_INIT_PO;
  }
}
