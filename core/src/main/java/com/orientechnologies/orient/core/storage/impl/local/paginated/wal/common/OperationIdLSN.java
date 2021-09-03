package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

public final class OperationIdLSN {
  public final int operationId;
  public final OLogSequenceNumber lsn;

  @Override
  public String toString() {
    return "OperationIdLSN{" + "operationId=" + operationId + ", lsn=" + lsn + '}';
  }

  public OperationIdLSN(int operationId, OLogSequenceNumber lsn) {
    this.operationId = operationId;
    this.lsn = lsn;
  }
}
