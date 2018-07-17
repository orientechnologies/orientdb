package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public class OSBTreeBonsaiBucketSetDeletedOperation extends OPageOperation {
  private int     pageOffset;
  private boolean isDeleted;

  public OSBTreeBonsaiBucketSetDeletedOperation() {
  }

  public OSBTreeBonsaiBucketSetDeletedOperation(OLogSequenceNumber pageLSN, long fileId, long pageIndex, int pageOffset,
      boolean isDeleted) {
    super(pageLSN, fileId, pageIndex);
    this.pageOffset = pageOffset;
    this.isDeleted = isDeleted;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_DELETED_OPERATION;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(pageOffset, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    content[offset] = isDeleted ? (byte) 1 : 0;
    offset++;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(pageOffset);
    buffer.put(isDeleted ? (byte) 1 : 0);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    pageOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    isDeleted = (content[offset] == 1);
    offset++;

    return offset;
  }

  public int getPageOffset() {
    return pageOffset;
  }

  public boolean isDeleted() {
    return isDeleted;
  }
}
