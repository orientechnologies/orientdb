package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v3.nullbucket;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v3.CellBTreeMultiValueV3NullBucket;

import java.nio.ByteBuffer;

public final class CellBTreeMultiValueV3NullBucketInitPO extends PageOperationRecord {
  private long mId;

  public CellBTreeMultiValueV3NullBucketInitPO() {
  }

  public CellBTreeMultiValueV3NullBucketInitPO(long mId) {
    this.mId = mId;
  }

  public long getmId() {
    return mId;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV3NullBucket bucket = new CellBTreeMultiValueV3NullBucket(cacheEntry);
    bucket.init(mId);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    //do nothing
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_NULL_BUCKET_MULTI_VALUE_V3_SET_RIGHT_SIBLING_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putLong(mId);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    mId = buffer.getLong();
  }
}
