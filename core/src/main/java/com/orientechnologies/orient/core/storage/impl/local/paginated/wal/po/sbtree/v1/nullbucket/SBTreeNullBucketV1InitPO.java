package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.nullbucket;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v1.OSBTreeNullBucketV1;

public class SBTreeNullBucketV1InitPO extends PageOperationRecord {
  public SBTreeNullBucketV1InitPO() {
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OSBTreeNullBucketV1 bucket = new OSBTreeNullBucketV1(cacheEntry);
    bucket.init();
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
  }

  @Override
  public int getId() {
    return WALRecordTypes.SBTREE_NULL_BUCKET_V1_INIT_PO;
  }
}
