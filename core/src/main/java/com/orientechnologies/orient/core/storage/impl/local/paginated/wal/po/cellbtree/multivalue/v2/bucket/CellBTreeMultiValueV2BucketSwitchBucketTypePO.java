package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2.CellBTreeMultiValueV2Bucket;

public class CellBTreeMultiValueV2BucketSwitchBucketTypePO extends PageOperationRecord {
  @Override
  public void redo(final OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV2Bucket bucket = new CellBTreeMultiValueV2Bucket(cacheEntry);
    bucket.switchBucketType();
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV2Bucket bucket = new CellBTreeMultiValueV2Bucket(cacheEntry);
    bucket.switchBucketType();
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_BUCKET_MULTI_VALUE_V2_SWITCH_BUCKET_TYPE_PO;
  }
}
