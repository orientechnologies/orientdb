package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public class OSBTreeBonsaiBucketUpdateValueOperation extends OPageOperation {
  private int    pageOffset;
  private int    entryIndex;
  private byte[] value;

  public OSBTreeBonsaiBucketUpdateValueOperation() {
  }

  public OSBTreeBonsaiBucketUpdateValueOperation(OLogSequenceNumber pageLSN, long fileId, long pageIndex, int pageOffset,
      int entryIndex, byte[] value) {
    super(pageLSN, fileId, pageIndex);
    this.pageOffset = pageOffset;
    this.entryIndex = entryIndex;
    this.value = value;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_UPDATE_VALUE_OPERATION;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE + value.length;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(pageOffset, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(entryIndex, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(value.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(value, 0, content, offset, value.length);
    offset += value.length;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(pageOffset);
    buffer.putInt(entryIndex);
    buffer.putInt(value.length);
    buffer.put(value);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    pageOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    entryIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int valueLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    value = new byte[valueLen];
    System.arraycopy(content, offset, value, 0, valueLen);
    offset += valueLen;

    return offset;
  }

  public int getPageOffset() {
    return pageOffset;
  }

  public int getEntryIndex() {
    return entryIndex;
  }

  public byte[] getValue() {
    return value;
  }
}
