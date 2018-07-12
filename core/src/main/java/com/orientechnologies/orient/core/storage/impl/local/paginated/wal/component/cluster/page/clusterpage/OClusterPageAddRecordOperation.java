package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public class OClusterPageAddRecordOperation extends OPageOperation {
  private int recordIndex;

  public OClusterPageAddRecordOperation() {
  }

  public OClusterPageAddRecordOperation(OLogSequenceNumber lsn, long fileId, long pageIndex, int recordIndex) {
    super(lsn, fileId, pageIndex);
    this.recordIndex = recordIndex;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_ADD_RECORD_OPERATION;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(recordIndex, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(recordIndex);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    recordIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  public int getRecordIndex() {
    return recordIndex;
  }
}
