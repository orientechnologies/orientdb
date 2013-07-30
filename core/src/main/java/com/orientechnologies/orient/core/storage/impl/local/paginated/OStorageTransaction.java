package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.tx.OTransaction;

/**
 * @author Andrey Lomakin
 * @since 12.06.13
 */
public class OStorageTransaction {
  private final OTransaction     clientTx;
  private final OOperationUnitId operationUnitId;

  private OLogSequenceNumber     startLSN;

  public OStorageTransaction(OTransaction clientTx, OOperationUnitId operationUnitId) {
    this.clientTx = clientTx;
    this.operationUnitId = operationUnitId;
  }

  public OTransaction getClientTx() {
    return clientTx;
  }

  public OOperationUnitId getOperationUnitId() {
    return operationUnitId;
  }

  public OLogSequenceNumber getStartLSN() {
    return startLSN;
  }

  public void setStartLSN(OLogSequenceNumber startLSN) {
    this.startLSN = startLSN;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OStorageTransaction that = (OStorageTransaction) o;

    if (!operationUnitId.equals(that.operationUnitId))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return operationUnitId.hashCode();
  }

  @Override
  public String toString() {
    return "OStorageTransaction{" + "clientTx=" + clientTx + ", operationUnitId=" + operationUnitId + ", startLSN=" + startLSN
        + "} ";
  }
}
