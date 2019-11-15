package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.bucket;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v1.OSBTreeBucketV1;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class SBTreeBucketV1AddAllPO extends PageOperationRecord {
  private int prevSize;

  private List<byte[]>      rawRecords;
  private boolean           isEncrypted;
  private OBinarySerializer keySerializer;
  private OBinarySerializer valueSerializer;

  public SBTreeBucketV1AddAllPO() {
  }

  public SBTreeBucketV1AddAllPO(int prevSize, List<byte[]> rawRecords, boolean isEncrypted, OBinarySerializer keySerializer,
      OBinarySerializer valueSerializer) {
    this.prevSize = prevSize;
    this.rawRecords = rawRecords;
    this.isEncrypted = isEncrypted;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public int getPrevSize() {
    return prevSize;
  }

  public List<byte[]> getRawRecords() {
    return rawRecords;
  }

  public boolean isEncrypted() {
    return isEncrypted;
  }

  public OBinarySerializer getKeySerializer() {
    return keySerializer;
  }

  public OBinarySerializer getValueSerializer() {
    return valueSerializer;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OSBTreeBucketV1 bucket = new OSBTreeBucketV1(cacheEntry);
    //noinspection unchecked
    bucket.addAll(rawRecords, isEncrypted, keySerializer, valueSerializer);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OSBTreeBucketV1 bucket = new OSBTreeBucketV1(cacheEntry);
    //noinspection unchecked
    bucket.shrink(prevSize, isEncrypted, keySerializer, valueSerializer);
  }

  @Override
  public int getId() {
    return WALRecordTypes.SBTREE_BUCKET_V1_ADD_ALL_PO;
  }

  @Override
  public int serializedSize() {
    int serializedSize =
        3 * OByteSerializer.BYTE_SIZE + 2 * OIntegerSerializer.INT_SIZE + OIntegerSerializer.INT_SIZE * rawRecords.size();
    for (final byte[] record : rawRecords) {
      serializedSize += record.length;
    }

    return super.serializedSize() + serializedSize;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(prevSize);

    buffer.put(isEncrypted ? (byte) 1 : 0);
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

    isEncrypted = buffer.get() > 0;
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
