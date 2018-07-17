package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public class OSBTreeBonsaiBucketSetTreeSizeOperation extends OPageOperation {
  private int  pageOffset;
  private long treeSize;

  public OSBTreeBonsaiBucketSetTreeSizeOperation() {
  }

  public OSBTreeBonsaiBucketSetTreeSizeOperation(OLogSequenceNumber pageLSN, long fileId, long pageIndex, int pageOffset,
      long treeSize) {
    super(pageLSN, fileId, pageIndex);

    this.pageOffset = pageOffset;
    this.treeSize = treeSize;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_SIZE_OPERATION;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(this.pageOffset, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serializeNative(treeSize, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(pageOffset);
    buffer.putLong(treeSize);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    pageOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    treeSize = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  public int getPageOffset() {
    return pageOffset;
  }

  public long getTreeSize() {
    return treeSize;
  }
}
