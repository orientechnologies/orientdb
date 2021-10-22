package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;

public final class MilestoneWALRecord implements OWALRecord {
  private int distance = -1;
  private int diskSize = -1;

  private volatile OLogSequenceNumber logSequenceNumber;

  @Override
  public OLogSequenceNumber getLsn() {
    return logSequenceNumber;
  }

  @Override
  public void setLsn(OLogSequenceNumber lsn) {
    this.logSequenceNumber = lsn;
  }

  @Override
  public void setDistance(int distance) {
    this.distance = distance;
  }

  @Override
  public void setDiskSize(int diskSize) {
    this.diskSize = diskSize;
  }

  @Override
  public int getDistance() {
    if (distance < 0) {
      throw new IllegalStateException("Distance is not set");
    }

    return distance;
  }

  @Override
  public int getDiskSize() {
    if (diskSize < 0) {
      throw new IllegalStateException("Disk size is not set");
    }

    return diskSize;
  }

  @Override
  public String toString() {
    return "MilestoneWALRecord{ operation_id_lsn = " + logSequenceNumber + '}';
  }
}
