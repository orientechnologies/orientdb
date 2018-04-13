package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAbstractWALRecord;

public class OEmptyWALRecord extends OAbstractWALRecord {
  public OEmptyWALRecord() {
  }

  @Override
  public int toStream(byte[] content, int offset) {
    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    return offset;
  }

  @Override
  public int serializedSize() {
    return 0;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }
}
