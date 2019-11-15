package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.nullbucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.HashIndexNullBucketV2;

import java.nio.ByteBuffer;

public final class LocalHashTableV2NullBucketRemoveValuePO extends PageOperationRecord {
  private byte[] prevValue;

  public LocalHashTableV2NullBucketRemoveValuePO() {
  }

  public LocalHashTableV2NullBucketRemoveValuePO(byte[] prevValue) {
    this.prevValue = prevValue;
  }

  public byte[] getPrevValue() {
    return prevValue;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final HashIndexNullBucketV2 bucket = new HashIndexNullBucketV2(cacheEntry);
    bucket.removeValue(prevValue);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final HashIndexNullBucketV2 bucket = new HashIndexNullBucketV2(cacheEntry);
    bucket.setValue(prevValue, null);
  }

  @Override
  public int getId() {
    return WALRecordTypes.LOCAL_HASH_TABLE_V2_NULL_BUCKET_REMOVE_VALUE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + prevValue.length;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(prevValue.length);
    buffer.put(prevValue);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    final int prevValueLen = buffer.getInt();
    prevValue = new byte[prevValueLen];
    buffer.get(prevValue);
  }
}
