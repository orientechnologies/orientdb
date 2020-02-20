package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

import com.orientechnologies.common.serialization.types.OBooleanSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFuzzyCheckpointStartRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;
import java.util.Optional;

public class OFuzzyCheckpointStartMetadataRecord extends OFuzzyCheckpointStartRecord {

  private Optional<byte[]> metadata = Optional.empty();

  public OFuzzyCheckpointStartMetadataRecord() {
  }

  public OFuzzyCheckpointStartMetadataRecord(OLogSequenceNumber previousCheckpoint, Optional<byte[]> lastMetadata,
      OLogSequenceNumber flushedLsn) {
    super(previousCheckpoint, flushedLsn);
    this.metadata = lastMetadata;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);
    if (metadata.isPresent()) {
      OBooleanSerializer.INSTANCE.serializeNative(true, content, offset);
      offset += OBooleanSerializer.BOOLEAN_SIZE;
      OIntegerSerializer.INSTANCE.serializeNative(metadata.get().length, content, offset);
      offset += OIntegerSerializer.INT_SIZE;
      System.arraycopy(metadata.get(), 0, content, offset, metadata.get().length);
    } else {
      OBooleanSerializer.INSTANCE.serializeNative(false, content, offset);
      offset += OBooleanSerializer.BOOLEAN_SIZE;
    }
    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);
    if (metadata.isPresent()) {
      buffer.put((byte) 1);
      buffer.putInt(metadata.get().length);
      buffer.put(metadata.get());
    } else {
      buffer.put((byte) 0);
    }
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);
    boolean metadata = OBooleanSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OBooleanSerializer.BOOLEAN_SIZE;
    if (metadata) {
      int size = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;
      byte[] meta = new byte[size];
      System.arraycopy(content, offset, meta, 0, size);
      this.metadata = Optional.of(meta);
      offset += size;
    } else {
      this.metadata = Optional.empty();
    }

    return offset;
  }

  @Override
  public int serializedSize() {
    int size = super.serializedSize();
    size += OBooleanSerializer.BOOLEAN_SIZE;
    if (metadata.isPresent()) {
      size += OIntegerSerializer.INT_SIZE;
      size += metadata.get().length;
    }
    return size;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.FUZZY_CHECKPOINT_START_METADATA_RECORD;
  }

  @Override
  public String toString() {
    return "OFuzzyCheckpointStartMetadataRecord{" + "metadata=" + metadata + ", lsn=" + getLsn() + '}';
  }

  public Optional<byte[]> getMetadata() {
    return metadata;
  }
}
