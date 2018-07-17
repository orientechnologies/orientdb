package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;

import java.nio.ByteBuffer;

public class OSBTreeBonsaiBucketSetRightSiblingOperation extends OPageOperation {
  private int                  pageOffset;
  private OBonsaiBucketPointer rightSibling;

  public OSBTreeBonsaiBucketSetRightSiblingOperation() {
  }

  public OSBTreeBonsaiBucketSetRightSiblingOperation(OLogSequenceNumber pageLSN, long fileId, long pageIndex, int pageOffset,
      OBonsaiBucketPointer rightSibling) {
    super(pageLSN, fileId, pageIndex);
    this.pageOffset = pageOffset;
    this.rightSibling = rightSibling;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_RIGHT_SIBLING_OPERATION;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 4 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(rightSibling.getVersion(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(rightSibling.getPageIndex(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(rightSibling.getPageOffset(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(pageOffset, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(rightSibling.getVersion());
    buffer.putInt(rightSibling.getPageIndex());
    buffer.putInt(rightSibling.getPageOffset());

    buffer.putInt(pageOffset);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    int ver = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int pi = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int po = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    rightSibling = new OBonsaiBucketPointer(pi, po, ver);

    pageOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  public int getPageOffset() {
    return pageOffset;
  }

  public OBonsaiBucketPointer getRightSibling() {
    return rightSibling;
  }
}
