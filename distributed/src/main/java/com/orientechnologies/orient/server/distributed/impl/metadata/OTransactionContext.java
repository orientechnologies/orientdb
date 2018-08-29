package com.orientechnologies.orient.server.distributed.impl.metadata;

import com.orientechnologies.orient.core.tx.OTransactionInternal;

public class OTransactionContext {
  private OTransactionInternal transaction;

  public OTransactionContext(OTransactionInternal tx) {
    transaction = tx;
  }

  public OTransactionInternal getTransaction() {
    return transaction;
  }
}
