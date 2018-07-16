package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.page.sbtreebucket;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public class OSBTreeBucketSetValueFreeListFirstIndexOperation extends OPageOperation {
  private long oldPageIndex;

  public OSBTreeBucketSetValueFreeListFirstIndexOperation() {
  }

  public OSBTreeBucketSetValueFreeListFirstIndexOperation(OLogSequenceNumber pageLSN, long fileId, long pageIndex,
      long oldPageIndex) {
    super(pageLSN, fileId, pageIndex);
    this.oldPageIndex = oldPageIndex;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_SET_VALUE_FREE_LIST_FIRST_INDEX_OPERATION;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(oldPageIndex, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putLong(oldPageIndex);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    oldPageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  public long getOldPageIndex() {
    return oldPageIndex;
  }
}
