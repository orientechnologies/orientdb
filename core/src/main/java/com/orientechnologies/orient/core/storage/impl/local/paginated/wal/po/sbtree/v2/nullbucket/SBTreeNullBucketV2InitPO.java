package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.nullbucket;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v2.OSBTreeNullBucketV2;

public class SBTreeNullBucketV2InitPO extends PageOperationRecord {
  public SBTreeNullBucketV2InitPO() {
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OSBTreeNullBucketV2 bucket = new OSBTreeNullBucketV2(cacheEntry);
    bucket.init();
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
  }

  @Override
  public int getId() {
    return WALRecordTypes.SBTREE_NULL_BUCKET_V2_INIT_PO;
  }
}
