package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;

import java.nio.ByteBuffer;

public class OSBTreeBonsaiBucketSetFreeListPointerOperation extends OPageOperation {
  private int                  pageOffset;
  private OBonsaiBucketPointer bucketPointer;

  public OSBTreeBonsaiBucketSetFreeListPointerOperation() {
  }

  public OSBTreeBonsaiBucketSetFreeListPointerOperation(OLogSequenceNumber pageLSN, long fileId, long pageIndex, int pageOffset,
      OBonsaiBucketPointer bucketPointer) {
    super(pageLSN, fileId, pageIndex);
    this.bucketPointer = bucketPointer;
    this.pageOffset = pageOffset;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_FREE_LIST_POINTER_OPERATION;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 4 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(bucketPointer.getVersion(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(bucketPointer.getPageIndex(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(bucketPointer.getPageOffset(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(pageOffset, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(bucketPointer.getVersion());
    buffer.putInt(bucketPointer.getPageIndex());
    buffer.putInt(bucketPointer.getPageOffset());

    buffer.putInt(pageOffset);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    int version = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int pageIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int pageOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    bucketPointer = new OBonsaiBucketPointer(pageIndex, pageOffset, version);

    this.pageOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  public OBonsaiBucketPointer getBucketPointer() {
    return bucketPointer;
  }

  public int getPageOffset() {
    return pageOffset;
  }
}
