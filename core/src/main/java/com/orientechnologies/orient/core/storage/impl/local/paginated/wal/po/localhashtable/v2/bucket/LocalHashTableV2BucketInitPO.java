package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.bucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.HashIndexBucketV2;

import java.nio.ByteBuffer;

public final class LocalHashTableV2BucketInitPO extends PageOperationRecord {
  private int depth;

  public LocalHashTableV2BucketInitPO() {
  }

  public LocalHashTableV2BucketInitPO(int depth) {
    this.depth = depth;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final HashIndexBucketV2 bucket = new HashIndexBucketV2(cacheEntry);
    bucket.init(depth);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
  }

  @Override
  public int getId() {
    return WALRecordTypes.LOCAL_HASH_TABLE_V2_BUCKET_INIT_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(depth);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    depth = buffer.getInt();
  }
}
