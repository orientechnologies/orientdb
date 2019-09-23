package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.bucket;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v2.OSBTreeBucketV2;

public class SBTreeBucketV2SwitchBucketTypePO extends PageOperationRecord {
  public SBTreeBucketV2SwitchBucketTypePO() {
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OSBTreeBucketV2 bucket = new OSBTreeBucketV2(cacheEntry);
    bucket.switchBucketType();
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OSBTreeBucketV2 bucket = new OSBTreeBucketV2(cacheEntry);
    bucket.switchBucketType();
  }

  @Override
  public int getId() {
    return WALRecordTypes.SBTREE_BUCKET_V2_SWITCH_BUCKET_TYPE_PO;
  }
}
