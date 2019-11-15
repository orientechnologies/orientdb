package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.bucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.HashIndexBucketV2;

import java.nio.ByteBuffer;

public final class LocalHashTableV2BucketUpdateEntryPO extends PageOperationRecord {
  private int    keySize;
  private int    index;
  private byte[] value;
  private byte[] oldValue;

  public LocalHashTableV2BucketUpdateEntryPO() {
  }

  public LocalHashTableV2BucketUpdateEntryPO(int index, byte[] value, byte[] oldValue, int keySize) {
    this.index = index;
    this.value = value;
    this.oldValue = oldValue;
    this.keySize = keySize;
  }

  public int getKeySize() {
    return keySize;
  }

  public int getIndex() {
    return index;
  }

  public byte[] getValue() {
    return value;
  }

  public byte[] getOldValue() {
    return oldValue;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final HashIndexBucketV2 bucket = new HashIndexBucketV2(cacheEntry);
    bucket.updateEntry(index, value, oldValue, keySize);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final HashIndexBucketV2 bucket = new HashIndexBucketV2(cacheEntry);
    bucket.updateEntry(index, oldValue, value, keySize);
  }

  @Override
  public int getId() {
    return WALRecordTypes.LOCAL_HASH_TABLE_V2_BUCKET_UPDATE_ENTRY_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 4 * OIntegerSerializer.INT_SIZE + value.length + oldValue.length;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(keySize);
    buffer.putInt(index);

    buffer.putInt(value.length);
    buffer.put(value);

    buffer.putInt(oldValue.length);
    buffer.put(oldValue);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    keySize = buffer.getInt();
    index = buffer.getInt();

    final int valueLen = buffer.getInt();
    value = new byte[valueLen];
    buffer.get(value);

    final int oldValueLen = buffer.getInt();
    oldValue = new byte[oldValueLen];
    buffer.get(oldValue);
  }
}
