package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.sbtreebonsai;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.OComponentOperationRecord;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OSBTreeBonsaiCreateCO extends OComponentOperationRecord {
  private String fileName;

  private long requiredFileId;
  private int  requiredPageIndex;
  private int  requiredPageOffset;

  public OSBTreeBonsaiCreateCO() {
  }

  public OSBTreeBonsaiCreateCO(String fileName, long requiredFileId, int requiredPageIndex, int requiredPageOffset) {
    this.fileName = fileName;

    this.requiredFileId = requiredFileId;
    this.requiredPageIndex = requiredPageIndex;
    this.requiredPageOffset = requiredPageOffset;
  }

  @Override
  public void redo(OAbstractPaginatedStorage storage) throws IOException {
    storage.addRidBagInternal(fileName, requiredFileId, requiredPageIndex, requiredPageOffset);
  }

  @Override
  public void undo(OAbstractPaginatedStorage storage) throws IOException {
    storage.deleteRidBagInternal(requiredFileId, requiredPageIndex, requiredPageOffset);
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    OStringSerializer.INSTANCE.serializeInByteBufferObject(fileName, buffer);

    buffer.putLong(requiredFileId);
    buffer.putInt(requiredPageIndex);
    buffer.putInt(requiredPageOffset);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    this.fileName = OStringSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);

    requiredFileId = buffer.getLong();
    requiredPageIndex = buffer.getInt();
    requiredPageOffset = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE + 2 * OIntegerSerializer.INT_SIZE + OStringSerializer.INSTANCE
        .getObjectSize(fileName);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_CREATE_CO;
  }
}
