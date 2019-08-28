package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.bucket;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1.OCellBTreeBucketSingleValueV1;

public class CellBTreeBucketSingleValueV1SwitchBucketTypePO extends PageOperationRecord {
  public CellBTreeBucketSingleValueV1SwitchBucketTypePO() {
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OCellBTreeBucketSingleValueV1 bucket = new OCellBTreeBucketSingleValueV1(cacheEntry);
    bucket.switchBucketType();
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OCellBTreeBucketSingleValueV1 bucket = new OCellBTreeBucketSingleValueV1(cacheEntry);
    bucket.switchBucketType();
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CELL_BTREE_BUCKET_SINGLE_VALUE_V1_SWITCH_BUCKET_TYPE_PO;
  }
}
