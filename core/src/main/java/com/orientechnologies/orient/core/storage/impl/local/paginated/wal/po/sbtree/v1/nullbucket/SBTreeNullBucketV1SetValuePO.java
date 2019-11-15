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

public class SBTreeNullBucketV1SetValuePO extends PageOperationRecord {
  private byte[] prevValue;
  private byte[] value;

  private OBinarySerializer valueSerializer;

  public SBTreeNullBucketV1SetValuePO() {
  }

  public SBTreeNullBucketV1SetValuePO(byte[] prevValue, byte[] value, OBinarySerializer valueSerializer) {
    this.prevValue = prevValue;
    this.value = value;
    this.valueSerializer = valueSerializer;
  }

  public byte[] getPrevValue() {
    return prevValue;
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
    bucket.setValue(value, valueSerializer);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OSBTreeNullBucketV1 bucket = new OSBTreeNullBucketV1(cacheEntry);
    if (prevValue == null) {
      //noinspection unchecked
      bucket.removeValue(valueSerializer);
    } else {
      //noinspection unchecked
      bucket.setValue(prevValue, valueSerializer);
    }
  }

  @Override
  public int getId() {
    return WALRecordTypes.SBTREE_NULL_BUCKET_V1_SET_VALUE_PO;
  }

  @Override
  public int serializedSize() {
    int size = 2 * OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE + value.length;
    if (prevValue != null) {
      size += prevValue.length + OIntegerSerializer.INT_SIZE;
    }

    return super.serializedSize() + size;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(value.length);
    buffer.put(value);

    buffer.put(valueSerializer.getId());
    buffer.put(prevValue == null ? (byte) 0 : 1);
    if (prevValue != null) {
      buffer.putInt(prevValue.length);
      buffer.put(prevValue);
    }
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    final int valueLength = buffer.getInt();
    value = new byte[valueLength];

    buffer.get(value);
    valueSerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(buffer.get());

    if (buffer.get() > 0) {
      final int prevValueLen = buffer.getInt();
      prevValue = new byte[prevValueLen];
      buffer.get(prevValue);
    }
  }
}
