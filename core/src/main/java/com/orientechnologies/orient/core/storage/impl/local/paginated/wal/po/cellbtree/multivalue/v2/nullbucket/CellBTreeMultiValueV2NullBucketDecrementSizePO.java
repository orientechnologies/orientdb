package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.nullbucket;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2.CellBTreeMultiValueV2NullBucket;

public class CellBTreeMultiValueV2NullBucketDecrementSizePO extends PageOperationRecord {

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV2NullBucket bucket = new CellBTreeMultiValueV2NullBucket(cacheEntry);
    bucket.decrementSize();
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV2NullBucket bucket = new CellBTreeMultiValueV2NullBucket(cacheEntry);
    bucket.incrementSize();
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_NULL_BUCKET_MULTI_VALUE_V2_DECREMENT_SIZE_PO;
  }
}
