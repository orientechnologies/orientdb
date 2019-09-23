package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.bucket;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1.CellBTreeBucketSingleValueV1;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class CellBTreeBucketSingleValueV1ShrinkPO extends PageOperationRecord {
  private int               newSize;
  private List<byte[]>      removedRecords;
  private boolean           isEncrypted;
  private OBinarySerializer keySerializer;

  public CellBTreeBucketSingleValueV1ShrinkPO() {
  }

  public CellBTreeBucketSingleValueV1ShrinkPO(int newSize, List<byte[]> removedRecords, boolean isEncrypted,
      OBinarySerializer keySerializer) {
    this.newSize = newSize;
    this.removedRecords = removedRecords;
    this.isEncrypted = isEncrypted;
    this.keySerializer = keySerializer;
  }

  public int getNewSize() {
    return newSize;
  }

  public List<byte[]> getRemovedRecords() {
    return removedRecords;
  }

  public boolean isEncrypted() {
    return isEncrypted;
  }

  public OBinarySerializer getKeySerializer() {
    return keySerializer;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeBucketSingleValueV1 bucket = new CellBTreeBucketSingleValueV1(cacheEntry);
    //noinspection unchecked
    bucket.shrink(newSize, isEncrypted, keySerializer);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeBucketSingleValueV1 bucket = new CellBTreeBucketSingleValueV1(cacheEntry);
    //noinspection unchecked
    bucket.addAll(removedRecords, isEncrypted, keySerializer);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_BUCKET_SINGLE_VALUE_V1_SHRINK_PO;
  }

  @Override
  public int serializedSize() {
    int serializedSize =
        2 * OIntegerSerializer.INT_SIZE + 2 * OByteSerializer.BYTE_SIZE + removedRecords.size() * OIntegerSerializer.INT_SIZE;
    for (final byte[] record : removedRecords) {
      serializedSize += record.length;
    }

    return super.serializedSize() + serializedSize;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(newSize);
    buffer.putInt(removedRecords.size());

    for (final byte[] record : removedRecords) {
      buffer.putInt(record.length);
      buffer.put(record);
    }

    buffer.put(isEncrypted ? (byte) 1 : 0);
    buffer.put(keySerializer.getId());
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    newSize = buffer.getInt();
    final int records = buffer.getInt();

    removedRecords = new ArrayList<>();
    for (int i = 0; i < records; i++) {
      final int recordLen = buffer.getInt();
      final byte[] record = new byte[recordLen];
      buffer.get(record);

      removedRecords.add(record);
    }

    isEncrypted = buffer.get() > 0;
    keySerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(buffer.get());
  }
}
