package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.sbteebonsai;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.OComponentOperationRecord;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OSBTreeBonsaiDeleteComponentCO extends OComponentOperationRecord {
  private String fileName;
  private long   requiredFileId;

  public OSBTreeBonsaiDeleteComponentCO() {
  }

  public OSBTreeBonsaiDeleteComponentCO(String fileName, long requiredFileId) {
    this.fileName = fileName;
    this.requiredFileId = requiredFileId;
  }

  @Override
  public void redo(OAbstractPaginatedStorage storage) throws IOException {
    storage.deleteRidBagComponentInternal(requiredFileId);
  }

  @Override
  public void undo(OAbstractPaginatedStorage storage) throws IOException {
    storage.addRidBagComponentInternal(fileName, requiredFileId);
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    OStringSerializer.INSTANCE.serializeInByteBufferObject(fileName, buffer);
    buffer.putLong(requiredFileId);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    this.fileName = OStringSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);
    requiredFileId = buffer.getLong();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE + OStringSerializer.INSTANCE.getObjectSize(fileName);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_DELETE_COMPONENT_CO;
  }
}
