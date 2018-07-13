package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpositionmapbucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public class OClusterPositionMapBucketMakeAvailableOperation extends OPageOperation {
  private int recordIndex;
  private int size;

  public OClusterPositionMapBucketMakeAvailableOperation() {
  }

  public OClusterPositionMapBucketMakeAvailableOperation(OLogSequenceNumber pageLSN, long fileId, long pageIndex, int recordIndex,
      int size) {
    super(pageLSN, fileId, pageIndex);

    this.recordIndex = recordIndex;
    this.size = size;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_BUCKET_MAKE_AVAILABLE_OPERATION;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(recordIndex, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(size, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(recordIndex);
    buffer.putInt(size);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    recordIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    size = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  public int getRecordIndex() {
    return recordIndex;
  }

  public int getSize() {
    return size;
  }
}
