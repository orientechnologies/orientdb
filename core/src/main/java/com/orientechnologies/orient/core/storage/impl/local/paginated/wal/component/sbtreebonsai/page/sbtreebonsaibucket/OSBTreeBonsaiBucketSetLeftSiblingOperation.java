package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;

import java.nio.ByteBuffer;

public class OSBTreeBonsaiBucketSetLeftSiblingOperation extends OPageOperation {
  private int                  pageOffset;
  private OBonsaiBucketPointer leftSibling;

  public OSBTreeBonsaiBucketSetLeftSiblingOperation() {
  }

  public OSBTreeBonsaiBucketSetLeftSiblingOperation(OLogSequenceNumber pageLSN, long fileId, long pageIndex, int pageOffset,
      OBonsaiBucketPointer leftSibling) {
    super(pageLSN, fileId, pageIndex);
    this.pageOffset = pageOffset;
    this.leftSibling = leftSibling;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_LEFT_SIBLING_OPERATION;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 4 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(leftSibling.getPageOffset(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(leftSibling.getPageIndex(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(leftSibling.getVersion(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(pageOffset, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(leftSibling.getPageOffset());
    buffer.putInt(leftSibling.getPageIndex());
    buffer.putInt(leftSibling.getVersion());

    buffer.putInt(pageOffset);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    int poff = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int pageIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int version = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    leftSibling = new OBonsaiBucketPointer(pageIndex, poff, version);

    pageOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  public int getPageOffset() {
    return pageOffset;
  }

  public OBonsaiBucketPointer getLeftSibling() {
    return leftSibling;
  }
}
