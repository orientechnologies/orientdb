package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.paginatedclusterstate;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public class OPaginatedClusterStateSetRecordSizeOperation extends OPageOperation {
  private long recordSize;

  public OPaginatedClusterStateSetRecordSizeOperation() {
  }

  public OPaginatedClusterStateSetRecordSizeOperation(OLogSequenceNumber pageLSN, long fileId, long pageIndex, long recordSize) {
    super(pageLSN, fileId, pageIndex);
    this.recordSize = recordSize;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.PAGINATED_CLUSTER_STATE_SET_RECORD_SIZE_OPERATION;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(recordSize, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putLong(recordSize);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    recordSize = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  public long getRecordSize() {
    return recordSize;
  }
}
