package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.sql.parser.OInteger;

import java.nio.ByteBuffer;

public class OAtomicUnitStartMetadataRecord extends OAtomicUnitStartRecordV2 {

  private byte[] metadata;

  public OAtomicUnitStartMetadataRecord() {
  }

  public OAtomicUnitStartMetadataRecord(final boolean isRollbackSupported, final long unitId, byte[] metadata) {
    super(isRollbackSupported, unitId);
    this.metadata = metadata;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);
    int size = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;
    metadata = new byte[size];
    System.arraycopy(metadata, 0, content, offset, size);
    offset += size;
    return offset;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);
    OIntegerSerializer.INSTANCE.serializeNative(metadata.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;
    System.arraycopy(metadata, 0, content, offset, metadata.length);
    offset += metadata.length;
    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + metadata.length;
  }

  public byte[] getMetadata() {
    return metadata;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.ATOMIC_UNIT_START_METADATA_RECORD;
  }
}
