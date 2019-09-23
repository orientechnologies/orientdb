package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.bucket;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v1.OSBTreeBucketV1;

public class SBTreeBucketV1SwitchBucketTypePO extends PageOperationRecord {
  public SBTreeBucketV1SwitchBucketTypePO() {
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OSBTreeBucketV1 bucket = new OSBTreeBucketV1(cacheEntry);
    bucket.switchBucketType();
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OSBTreeBucketV1 bucket = new OSBTreeBucketV1(cacheEntry);
    bucket.switchBucketType();
  }

  @Override
  public int getId() {
    return WALRecordTypes.SBTREE_BUCKET_V1_SWITCH_BUCKET_TYPE_PO;
  }
}
