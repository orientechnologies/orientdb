package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.bucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1.CellBTreeBucketSingleValueV1;

import java.nio.ByteBuffer;

public final class CellBTreeBucketSingleValueV1UpdateValuePO extends PageOperationRecord {
  private int index;
  private int keySize;

  private byte[] prevValue;
  private byte[] value;

  public CellBTreeBucketSingleValueV1UpdateValuePO() {
  }

  public CellBTreeBucketSingleValueV1UpdateValuePO(int index, int keySize, byte[] prevValue, byte[] value) {
    this.index = index;
    this.keySize = keySize;
    this.prevValue = prevValue;
    this.value = value;
  }

  public int getIndex() {
    return index;
  }

  public int getKeySize() {
    return keySize;
  }

  public byte[] getPrevValue() {
    return prevValue;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeBucketSingleValueV1 bucket = new CellBTreeBucketSingleValueV1(cacheEntry);
    bucket.updateValue(index, value, keySize);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeBucketSingleValueV1 bucket = new CellBTreeBucketSingleValueV1(cacheEntry);
    bucket.updateValue(index, prevValue, keySize);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_BUCKET_SINGLE_VALUE_V1_UPDATE_VALUE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 4 * OIntegerSerializer.INT_SIZE + prevValue.length + value.length;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(index);
    buffer.putInt(keySize);

    buffer.putInt(prevValue.length);
    buffer.put(prevValue);

    buffer.putInt(value.length);
    buffer.put(value);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    index = buffer.getInt();
    keySize = buffer.getInt();

    final int prevValueLen = buffer.getInt();
    prevValue = new byte[prevValueLen];
    buffer.get(prevValue);

    final int valueLen = buffer.getInt();
    value = new byte[valueLen];
    buffer.get(value);
  }
}
