package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.bucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.HashIndexBucketV2;

import java.nio.ByteBuffer;

public final class LocalHashTableV2BucketDeleteEntryPO extends PageOperationRecord {
  private int    index;
  private long   hashCode;
  private byte[] key;
  private byte[] value;

  public LocalHashTableV2BucketDeleteEntryPO() {
  }

  public LocalHashTableV2BucketDeleteEntryPO(int index, long hashCode, byte[] key, byte[] value) {
    this.index = index;
    this.hashCode = hashCode;
    this.key = key;
    this.value = value;
  }

  public int getIndex() {
    return index;
  }

  public long getHashCode() {
    return hashCode;
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final HashIndexBucketV2 bucket = new HashIndexBucketV2(cacheEntry);
    bucket.deleteEntry(index, hashCode, key, value);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final HashIndexBucketV2 bucket = new HashIndexBucketV2(cacheEntry);
    final boolean result = bucket.addEntry(index, hashCode, key, value);
    if (!result) {
      throw new IllegalStateException("Can not undo delete entry operation");
    }
  }

  @Override
  public int getId() {
    return WALRecordTypes.LOCAL_HASH_TABLE_V2_BUCKET_DELETE_ENTRY_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE + 3 * OIntegerSerializer.INT_SIZE + key.length + value.length;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(index);
    buffer.putLong(hashCode);

    buffer.putInt(key.length);
    buffer.put(key);

    buffer.putInt(value.length);
    buffer.put(value);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    index = buffer.getInt();
    hashCode = buffer.getLong();

    final int keyLen = buffer.getInt();
    key = new byte[keyLen];
    buffer.get(key);

    final int valueLen = buffer.getInt();
    value = new byte[valueLen];
    buffer.get(value);
  }
}
