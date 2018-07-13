package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.paginatedclusterstate;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public class OPaginatedClusterStateSetFreeListPageOperation extends OPageOperation {
  private int  index;
  private long oldPageIndex;

  public OPaginatedClusterStateSetFreeListPageOperation() {
  }

  public OPaginatedClusterStateSetFreeListPageOperation(OLogSequenceNumber pageLSN, long fileId, long pageIndex, int index,
      long oldPageIndex) {
    super(pageLSN, fileId, pageIndex);
    this.index = index;
    this.oldPageIndex = oldPageIndex;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.PAGINATED_CLUSTER_STATE_SET_FREE_LIST_PAGE_OPERATION;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serializeNative(oldPageIndex, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(index);
    buffer.putLong(oldPageIndex);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    oldPageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  public int getIndex() {
    return index;
  }

  public long getOldPageIndex() {
    return oldPageIndex;
  }
}
