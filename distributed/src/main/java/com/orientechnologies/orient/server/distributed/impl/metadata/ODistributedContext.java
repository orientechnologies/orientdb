package com.orientechnologies.orient.server.distributed.impl.metadata;

import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ODistributedContext {

  private Map<OSessionOperationId, OTransactionContext> transactions;

  public ODistributedContext() {
    transactions = new ConcurrentHashMap<>();
  }

  public void registerTransaction(OSessionOperationId requestId, OTransactionInternal tx) {
    transactions.put(requestId, new OTransactionContext(tx));

  }

  public OTransactionContext getTransaction(OSessionOperationId operationId) {
    return transactions.get(operationId);
  }

  public void close(OSessionOperationId operationId) {
    transactions.remove(operationId);
  }
}
