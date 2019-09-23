package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.sbtreebonsai;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.OComponentOperationRecord;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OSBTreeBonsaiDeleteCO extends OComponentOperationRecord {
  private String fileName;

  private long fileId;

  private int pageIndex;
  private int pageOffset;

  public OSBTreeBonsaiDeleteCO() {
  }

  public OSBTreeBonsaiDeleteCO(String fileName, long fileId, int pageIndex, int pageOffset) {
    this.fileName = fileName;
    this.fileId = fileId;
    this.pageIndex = pageIndex;
    this.pageOffset = pageOffset;
  }

  @Override
  public void redo(OAbstractPaginatedStorage storage) throws IOException {
    storage.deleteRidBagInternal(fileId, pageIndex, pageOffset);
  }

  @Override
  public void undo(OAbstractPaginatedStorage storage) throws IOException {
    storage.addRidBagInternal(fileName, fileId, pageIndex, pageOffset);
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    OStringSerializer.INSTANCE.serializeInByteBufferObject(fileName, buffer);

    buffer.putLong(fileId);
    buffer.putInt(pageIndex);
    buffer.putInt(pageOffset);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    fileName = OStringSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);

    fileId = buffer.getLong();
    pageIndex = buffer.getInt();
    pageOffset = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE + 2 * OIntegerSerializer.INT_SIZE + OStringSerializer.INSTANCE
        .getObjectSize(fileName);
  }

  @Override
  public int getId() {
    return WALRecordTypes.SBTREE_BONSAI_DELETE_CO;
  }
}
