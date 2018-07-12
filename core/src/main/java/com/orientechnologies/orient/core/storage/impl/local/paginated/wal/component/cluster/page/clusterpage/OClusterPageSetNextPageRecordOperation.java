package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpage;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public class OClusterPageSetNextPageRecordOperation extends OPageOperation {
  private long nextPage;

  public OClusterPageSetNextPageRecordOperation() {
  }

  public OClusterPageSetNextPageRecordOperation(OLogSequenceNumber lsn, long fileId, long pageIndex, long nextPage) {
    super(lsn, fileId, pageIndex);
    this.nextPage = nextPage;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_SET_NEXT_PAGE_OPERATION;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(nextPage, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putLong(nextPage);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    nextPage = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  public long getNextPage() {
    return nextPage;
  }
}
