package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.sbtreebonsai;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.OComponentOperationRecord;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OSBTreeBonsaiCreateComponentCO extends OComponentOperationRecord {
  private String fileName;
  private long   fileId;

  public OSBTreeBonsaiCreateComponentCO() {
  }

  public OSBTreeBonsaiCreateComponentCO(String fileName, long fileId) {
    this.fileName = fileName;
    this.fileId = fileId;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    OStringSerializer.INSTANCE.serializeInByteBufferObject(fileName, buffer);

    buffer.putLong(fileId);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    fileName = OStringSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);
    fileId = buffer.getLong();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE + OStringSerializer.INSTANCE.getObjectSize(fileName);
  }

  @Override
  public void redo(OAbstractPaginatedStorage storage) throws IOException {
    storage.addRidBagComponentInternal(fileName, fileId);
  }

  @Override
  public void undo(OAbstractPaginatedStorage storage) throws IOException {
    storage.deleteRidBagComponentInternal(fileId);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_CREATE_COMPONENT_CO;
  }
}
