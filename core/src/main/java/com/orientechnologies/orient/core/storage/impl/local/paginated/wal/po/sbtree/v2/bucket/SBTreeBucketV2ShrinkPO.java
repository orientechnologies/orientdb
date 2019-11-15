package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.bucket;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v2.OSBTreeBucketV2;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class SBTreeBucketV2ShrinkPO extends PageOperationRecord {
  private int               newSize;
  private List<byte[]>      removedRecords;
  private OBinarySerializer keySerializer;
  private OBinarySerializer valueSerializer;

  public SBTreeBucketV2ShrinkPO() {
  }

  public SBTreeBucketV2ShrinkPO(int newSize, List<byte[]> removedRecords, OBinarySerializer keySerializer,
      OBinarySerializer valueSerializer) {
    this.newSize = newSize;
    this.removedRecords = removedRecords;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public int getNewSize() {
    return newSize;
  }

  public List<byte[]> getRemovedRecords() {
    return removedRecords;
  }

  public OBinarySerializer getKeySerializer() {
    return keySerializer;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OSBTreeBucketV2 bucket = new OSBTreeBucketV2(cacheEntry);
    //noinspection unchecked
    bucket.shrink(newSize, keySerializer, valueSerializer);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OSBTreeBucketV2 bucket = new OSBTreeBucketV2(cacheEntry);
    //noinspection unchecked
    bucket.addAll(removedRecords, keySerializer, valueSerializer);
  }

  @Override
  public int getId() {
    return WALRecordTypes.SBTREE_BUCKET_V2_SHRINK_PO;
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

    buffer.put(keySerializer.getId());
    buffer.put(valueSerializer.getId());
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

    keySerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(buffer.get());
    valueSerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(buffer.get());
  }
}
