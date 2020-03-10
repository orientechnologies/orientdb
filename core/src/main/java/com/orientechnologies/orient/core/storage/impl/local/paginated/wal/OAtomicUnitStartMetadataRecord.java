package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;

import java.nio.ByteBuffer;

public class OAtomicUnitStartMetadataRecord extends OAtomicUnitStartRecord {

  private byte[] metadata;

  public OAtomicUnitStartMetadataRecord() {
  }

  public OAtomicUnitStartMetadataRecord(final boolean isRollbackSupported, final OOperationUnitId unitId, byte[] metadata) {
    super(isRollbackSupported, unitId);
    this.metadata = metadata;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);
    buffer.putInt(metadata.length);
    buffer.put(metadata);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);
    int len = buffer.getInt();
    this.metadata = new byte[len];
    buffer.get(this.metadata);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + metadata.length;
  }

  public byte[] getMetadata() {
    return metadata;
  }

  @Override
  public int getId() {
    return WALRecordTypes.ATOMIC_UNIT_START_METADATA_RECORD;
  }
}
