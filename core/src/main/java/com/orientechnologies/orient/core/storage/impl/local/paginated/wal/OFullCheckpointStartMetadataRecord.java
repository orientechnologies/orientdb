/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OBooleanSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.sql.parser.OInteger;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/2/13
 */
public class OFullCheckpointStartMetadataRecord extends OFullCheckpointStartRecord {

  private Optional<byte[]> metadata = Optional.empty();

  public OFullCheckpointStartMetadataRecord() {
  }

  public OFullCheckpointStartMetadataRecord(final OLogSequenceNumber previousCheckpoint, Optional<byte[]> metadata) {
    super(previousCheckpoint);
    this.metadata = metadata;
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
    return WALRecordTypes.FULL_CHECKPOINT_START_METADATA_RECORD;
  }

  @Override
  public String toString() {
    return "OFullCheckpointStartMetadataRecord{" + "metadata=" + metadata + ", lsn=" + lsn + '}';
  }

  public Optional<byte[]> getMetadata() {
    return metadata;
  }
}
