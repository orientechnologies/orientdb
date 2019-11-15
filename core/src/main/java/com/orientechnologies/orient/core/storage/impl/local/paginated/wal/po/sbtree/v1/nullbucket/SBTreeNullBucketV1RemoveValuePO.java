package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.nullbucket;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v1.OSBTreeNullBucketV1;

import java.nio.ByteBuffer;

public final class SBTreeNullBucketV1RemoveValuePO extends PageOperationRecord {
  private byte[]            value;
  private OBinarySerializer valueSerializer;

  public SBTreeNullBucketV1RemoveValuePO() {
  }

  public SBTreeNullBucketV1RemoveValuePO(byte[] value, OBinarySerializer valueSerializer) {
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
    final OSBTreeNullBucketV1 bucket = new OSBTreeNullBucketV1(cacheEntry);
    //noinspection unchecked
    bucket.removeValue(valueSerializer);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OSBTreeNullBucketV1 bucket = new OSBTreeNullBucketV1(cacheEntry);
    //noinspection unchecked
    bucket.setValue(value, valueSerializer);
  }

  @Override
  public int getId() {
    return WALRecordTypes.SBTREE_NULL_BUCKET_V1_REMOVE_VALUE_PO;
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
