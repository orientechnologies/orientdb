package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v3.nullbucket;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v3.CellBTreeMultiValueV3NullBucket;

public class CellBTreeMultiValueV3NullBucketIncrementSizePO extends PageOperationRecord {

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV3NullBucket bucket = new CellBTreeMultiValueV3NullBucket(cacheEntry);
    bucket.incrementSize();
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV3NullBucket bucket = new CellBTreeMultiValueV3NullBucket(cacheEntry);
    bucket.decrementSize();
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_NULL_BUCKET_MULTI_VALUE_V3_INCREMENT_SIZE_PO;
  }
}
