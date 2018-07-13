package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpositionmapbucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public class OClusterPositionMapBucketSetOperation extends OPageOperation {
  private int  recordIndex;
  private byte flag;
  private long recordPageIndex;
  private int  recordPosition;

  public OClusterPositionMapBucketSetOperation() {
  }

  public OClusterPositionMapBucketSetOperation(OLogSequenceNumber pageLSN, long fileId, long pageIndex, int recordIndex, byte flag,
      long recordPageIndex, int recordPosition) {
    super(pageLSN, fileId, pageIndex);

    this.recordIndex = recordIndex;
    this.flag = flag;
    this.recordPageIndex = recordPageIndex;
    this.recordPosition = recordPosition;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_BUCKET_SET_OPERATION;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    content[offset] = flag;
    offset++;

    OIntegerSerializer.INSTANCE.serializeNative(recordIndex, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serializeNative(recordPageIndex, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(recordPosition, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.put(flag);
    buffer.putInt(recordIndex);
    buffer.putLong(recordPageIndex);
    buffer.putInt(recordPosition);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    flag = content[offset];
    offset++;

    recordIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    recordPageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    recordPosition = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  public int getRecordIndex() {
    return recordIndex;
  }

  public byte getFlag() {
    return flag;
  }

  public long getRecordPageIndex() {
    return recordPageIndex;
  }

  public int getRecordPosition() {
    return recordPosition;
  }
}
