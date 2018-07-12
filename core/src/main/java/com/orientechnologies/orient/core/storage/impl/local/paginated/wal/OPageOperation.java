package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OLongSerializer;

import java.nio.ByteBuffer;

public abstract class OPageOperation extends OAbstractWALRecord {
  private long               fileId;
  private long               pageIndex;
  private OLogSequenceNumber pageLSN;

  public OPageOperation() {
  }

  public OPageOperation(OLogSequenceNumber pageLSN, long fileId, long pageIndex) {
    this.fileId = fileId;
    this.pageIndex = pageIndex;
    this.pageLSN = pageLSN;
  }

  @Override
  public int serializedSize() {
    return 4 * OLongSerializer.LONG_SIZE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OLongSerializer.INSTANCE.serializeNative(fileId, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(pageIndex, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(pageLSN.getSegment(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(pageLSN.getPosition(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putLong(fileId);
    buffer.putLong(pageIndex);
    buffer.putLong(pageLSN.getSegment());
    buffer.putLong(pageLSN.getPosition());
  }

  public long getFileId() {
    return fileId;
  }

  public long getPageIndex() {
    return pageIndex;
  }

  public OLogSequenceNumber getPageLSN() {
    return pageLSN;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    fileId = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    pageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    final long segment = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    final long pageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    pageLSN = new OLogSequenceNumber(segment, pageIndex);

    return offset;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }
}
