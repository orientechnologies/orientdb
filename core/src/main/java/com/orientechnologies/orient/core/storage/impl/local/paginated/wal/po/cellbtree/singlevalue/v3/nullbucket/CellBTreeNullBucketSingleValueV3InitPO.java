package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.nullbucket;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueV3NullBucket;

public final class CellBTreeNullBucketSingleValueV3InitPO extends PageOperationRecord {
  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeSingleValueV3NullBucket bucket = new CellBTreeSingleValueV3NullBucket(cacheEntry);
    bucket.init();
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V3_INIT_PO;
  }
}
