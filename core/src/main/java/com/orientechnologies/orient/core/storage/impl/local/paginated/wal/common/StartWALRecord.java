package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;

public final class StartWALRecord implements OWALRecord {
  private volatile OLogSequenceNumber lsn;

  @Override
  public OLogSequenceNumber getLsn() {
    return lsn;
  }

  @Override
  public void setLsn(OLogSequenceNumber lsn) {
    this.lsn = lsn;
  }

  @Override
  public void setDistance(int distance) {}

  @Override
  public void setDiskSize(int diskSize) {}

  @Override
  public int getDistance() {
    return 0;
  }

  @Override
  public int getDiskSize() {
    return CASWALPage.RECORDS_OFFSET;
  }
}
