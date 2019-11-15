package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.bucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.HashIndexBucketV2;

import java.nio.ByteBuffer;

public final class LocalHashTableV2BucketSetDepthPO extends PageOperationRecord {
  private byte depth;
  private byte oldDepth;

  public LocalHashTableV2BucketSetDepthPO() {
  }

  public LocalHashTableV2BucketSetDepthPO(byte depth, byte oldDepth) {
    this.depth = depth;
    this.oldDepth = oldDepth;
  }

  public byte getDepth() {
    return depth;
  }

  public byte getOldDepth() {
    return oldDepth;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final HashIndexBucketV2 bucket = new HashIndexBucketV2(cacheEntry);
    bucket.setDepth(depth);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final HashIndexBucketV2 bucket = new HashIndexBucketV2(cacheEntry);
    bucket.setDepth(oldDepth);
  }

  @Override
  public int getId() {
    return WALRecordTypes.LOCAL_HASH_TABLE_V2_BUCKET_SET_DEPTH_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OByteSerializer.BYTE_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.put(depth);
    buffer.put(oldDepth);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    depth = buffer.get();
    oldDepth = buffer.get();
  }
}
