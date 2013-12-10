package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.tx.OTransaction;

/**
 * @author Andrey Lomakin
 * @since 12.06.13
 */
public class OStorageTransaction {
  private final OTransaction clientTx;

  public OStorageTransaction(OTransaction clientTx) {
    this.clientTx = clientTx;
  }

  public OTransaction getClientTx() {
    return clientTx;
  }
}
