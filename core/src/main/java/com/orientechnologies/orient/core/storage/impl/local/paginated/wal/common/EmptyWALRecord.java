package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAbstractWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import java.nio.ByteBuffer;

public final class EmptyWALRecord extends OAbstractWALRecord {
  public EmptyWALRecord() {}

  @Override
  public int toStream(final byte[] content, final int offset) {
    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {}

  @Override
  public int fromStream(final byte[] content, final int offset) {
    return offset;
  }

  @Override
  public int serializedSize() {
    return 0;
  }

  @Override
  public int getId() {
    return WALRecordTypes.EMPTY_WAL_RECORD;
  }
}
