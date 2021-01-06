package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import java.nio.ByteBuffer;

public final class MetaDataRecord extends OAbstractWALRecord {
  private byte[] metadata;

  public MetaDataRecord() {}

  public MetaDataRecord(final byte[] metadata) {
    this.metadata = metadata;
  }

  public byte[] getMetadata() {
    return metadata;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(metadata.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(metadata, 0, content, offset, metadata.length);

    return offset + content.length;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putInt(metadata.length);
    buffer.put(metadata);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    final int metadataLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    metadata = new byte[metadataLen];
    System.arraycopy(content, offset, metadata, 0, metadataLen);
    return offset + metadataLen;
  }

  @Override
  public int serializedSize() {
    return OIntegerSerializer.INT_SIZE + metadata.length;
  }

  @Override
  public int getId() {
    return WALRecordTypes.TX_METADATA;
  }
}
