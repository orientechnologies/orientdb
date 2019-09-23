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

public final class SBTreeBucketV2AddAllPO extends PageOperationRecord {
  private int prevSize;

  private List<byte[]>      rawRecords;
  private OBinarySerializer keySerializer;
  private OBinarySerializer valueSerializer;

  public SBTreeBucketV2AddAllPO() {
  }

  public SBTreeBucketV2AddAllPO(int prevSize, List<byte[]> rawRecords, OBinarySerializer keySerializer,
      OBinarySerializer valueSerializer) {
    this.prevSize = prevSize;
    this.rawRecords = rawRecords;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public int getPrevSize() {
    return prevSize;
  }

  public List<byte[]> getRawRecords() {
    return rawRecords;
  }

  public OBinarySerializer getKeySerializer() {
    return keySerializer;
  }

  public OBinarySerializer getValueSerializer() {
    return valueSerializer;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OSBTreeBucketV2 bucket = new OSBTreeBucketV2(cacheEntry);
    //noinspection unchecked
    bucket.addAll(rawRecords, keySerializer, valueSerializer);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OSBTreeBucketV2 bucket = new OSBTreeBucketV2(cacheEntry);
    //noinspection unchecked
    bucket.shrink(prevSize, keySerializer, valueSerializer);
  }

  @Override
  public int getId() {
    return WALRecordTypes.SBTREE_BUCKET_V2_ADD_ALL_PO;
  }

  @Override
  public int serializedSize() {
    int serializedSize =
        2 * OByteSerializer.BYTE_SIZE + 2 * OIntegerSerializer.INT_SIZE + OIntegerSerializer.INT_SIZE * rawRecords.size();
    for (final byte[] record : rawRecords) {
      serializedSize += record.length;
    }

    return super.serializedSize() + serializedSize;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(prevSize);

    buffer.put(keySerializer.getId());
    buffer.put(valueSerializer.getId());

    buffer.putInt(rawRecords.size());
    for (final byte[] record : rawRecords) {
      buffer.putInt(record.length);
      buffer.put(record);
    }
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    prevSize = buffer.getInt();

    keySerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(buffer.get());
    valueSerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(buffer.get());

    rawRecords = new ArrayList<>();
    final int recordsSize = buffer.getInt();
    for (int i = 0; i < recordsSize; i++) {
      final int recordSize = buffer.getInt();
      final byte[] record = new byte[recordSize];
      buffer.get(record);

      rawRecords.add(record);
    }
  }
}
