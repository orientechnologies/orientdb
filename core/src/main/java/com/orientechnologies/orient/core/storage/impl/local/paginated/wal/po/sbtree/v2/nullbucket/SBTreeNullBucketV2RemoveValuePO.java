package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.nullbucket;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v2.OSBTreeNullBucketV2;

import java.nio.ByteBuffer;

public final class SBTreeNullBucketV2RemoveValuePO extends PageOperationRecord {
  private byte[]            value;
  private OBinarySerializer valueSerializer;

  public SBTreeNullBucketV2RemoveValuePO() {
  }

  public SBTreeNullBucketV2RemoveValuePO(byte[] value, OBinarySerializer valueSerializer) {
    this.value = value;
    this.valueSerializer = valueSerializer;
  }

  public byte[] getValue() {
    return value;
  }

  public OBinarySerializer getValueSerializer() {
    return valueSerializer;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OSBTreeNullBucketV2 bucket = new OSBTreeNullBucketV2(cacheEntry);
    //noinspection unchecked
    bucket.removeValue(valueSerializer);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OSBTreeNullBucketV2 bucket = new OSBTreeNullBucketV2(cacheEntry);
    //noinspection unchecked
    bucket.setValue(value, valueSerializer);
  }

  @Override
  public int getId() {
    return WALRecordTypes.SBTREE_NULL_BUCKET_V2_REMOVE_VALUE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + value.length + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(value.length);
    buffer.put(value);

    buffer.put(valueSerializer.getId());
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    final int valueLen = buffer.getInt();
    value = new byte[valueLen];
    buffer.get(value);

    valueSerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(buffer.get());
  }
}
