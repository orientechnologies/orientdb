package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;

public final class MilestoneWALRecord implements OWALRecord {
  private int distance = -1;
  private int diskSize = -1;

  private volatile OperationIdLSN operationIdLSN;

  @Override
  public OLogSequenceNumber getLsn() {
    return operationIdLSN.lsn;
  }

  @Override
  public void setOperationIdLsn(OLogSequenceNumber lsn, int operationId) {
    this.operationIdLSN = new OperationIdLSN(operationId, lsn);
  }

  @Override
  public OperationIdLSN getOperationIdLSN() {
    return operationIdLSN;
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
  public boolean trackOperationId() {
    return false;
  }

  @Override
  public String toString() {
    return "MilestoneWALRecord{ operation_id_lsn = " + operationIdLSN + '}';
  }
}
