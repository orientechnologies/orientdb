package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.nullbucket;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.HashIndexNullBucketV2;

public final class LocalHashTableV2NullBucketInitPO extends PageOperationRecord {
  @Override
  public void redo(final OCacheEntry cacheEntry) {
    final HashIndexNullBucketV2 bucket = new HashIndexNullBucketV2(cacheEntry);
    bucket.init();
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    //do nothing
  }

  @Override
  public int getId() {
    return WALRecordTypes.LOCAL_HASH_TABLE_V2_NULL_BUCKET_INIT_PO;
  }
}
