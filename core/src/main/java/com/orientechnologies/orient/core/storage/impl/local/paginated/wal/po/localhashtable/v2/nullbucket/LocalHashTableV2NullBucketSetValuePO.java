package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.nullbucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.HashIndexNullBucketV2;

import java.nio.ByteBuffer;

public class LocalHashTableV2NullBucketSetValuePO extends PageOperationRecord {
  private byte[] pastValue;
  private byte[] value;

  public LocalHashTableV2NullBucketSetValuePO(byte[] pastValue, byte[] value) {
    this.pastValue = pastValue;
    this.value = value;
  }

  public LocalHashTableV2NullBucketSetValuePO() {
  }

  public byte[] getPastValue() {
    return pastValue;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final HashIndexNullBucketV2 bucket = new HashIndexNullBucketV2(cacheEntry);
    bucket.setValue(value, pastValue);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final HashIndexNullBucketV2 bucket = new HashIndexNullBucketV2(cacheEntry);
    if (pastValue == null) {
      bucket.removeValue();
    } else {
      bucket.setValue(pastValue, value);
    }
  }

  @Override
  public int getId() {
    return WALRecordTypes.LOCAL_HASH_TABLE_V2_NULL_BUCKET_SET_VALUE_PO;
  }

  @Override
  public int serializedSize() {
    int size = OIntegerSerializer.INT_SIZE + value.length + OByteSerializer.BYTE_SIZE;
    if (pastValue != null) {
      size += OIntegerSerializer.INT_SIZE + pastValue.length;
    }

    return super.serializedSize() + size;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(value.length);
    buffer.put(value);

    buffer.put(pastValue != null ? (byte) 1 : 0);
    if (pastValue != null) {
      buffer.putInt(pastValue.length);
      buffer.put(pastValue);
    }
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    final int valLen = buffer.getInt();
    value = new byte[valLen];
    buffer.get(value);

    if (buffer.get() > 0) {
      final int pastValueLen = buffer.getInt();
      pastValue = new byte[pastValueLen];
      buffer.get(pastValue);
    }
  }
}
