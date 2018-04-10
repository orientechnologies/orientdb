package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;

import java.util.concurrent.atomic.AtomicReference;

public class OMilestoneWALRecord implements OWALRecord {
  private volatile int distance = -1;
  private volatile int diskSize = -1;

  private final AtomicReference<OLogSequenceNumber> lsn = new AtomicReference<>();

  @Override
  public OLogSequenceNumber getLsn() {
    return lsn.get();
  }

  @Override
  public void setLsn(OLogSequenceNumber lsn) {
    this.lsn.set(lsn);
  }

  @Override
  public boolean casLSN(OLogSequenceNumber currentLSN, OLogSequenceNumber newLSN) {
    return this.lsn.compareAndSet(currentLSN, newLSN);
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
    return "OMilestoneWALRecord{" + "lsn=" + lsn + '}';
  }
}
