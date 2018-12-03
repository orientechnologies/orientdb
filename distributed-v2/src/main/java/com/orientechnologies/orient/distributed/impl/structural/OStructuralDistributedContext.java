package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.distributed.impl.OIncrementOperationalLog;
import com.orientechnologies.orient.distributed.impl.coordinator.*;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.metadata.OTransactionContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class OStructuralDistributedContext {
  private Map<OSessionOperationId, OTransactionContext> transactions;
  private OStructuralDistributedExecutor                executor;
  private OStructuralSubmitContext                      submitContext;
  private OStructuralCoordinator                        coordinator;
  private OOperationLog                                 opLog;

  public OStructuralDistributedContext() {
    transactions = new ConcurrentHashMap<>();
    executor = new OStructuralDistributedExecutor(Executors.newSingleThreadExecutor(), opLog, null);
    submitContext = new OStructuralSubmitContextImpl();
    coordinator = null;
    initOpLog();
  }

  private void initOpLog() {
//    this.opLog = OPersistentOperationalLogV1.newInstance(databaseName, context);
    this.opLog = new OIncrementOperationalLog();
  }

  public void registerTransaction(OSessionOperationId requestId, OTransactionInternal tx) {
    transactions.put(requestId, new OTransactionContext(tx));

  }

  public OTransactionContext getTransaction(OSessionOperationId operationId) {
    return transactions.get(operationId);
  }

  public void closeTransaction(OSessionOperationId operationId) {
    transactions.remove(operationId);
  }

  public OStructuralDistributedExecutor getExecutor() {
    return executor;
  }

  public OStructuralSubmitContext getSubmitContext() {
    return submitContext;
  }

  public synchronized OStructuralCoordinator getCoordinator() {
    return coordinator;
  }

  public synchronized void makeCoordinator(String nodeName) {
    if (coordinator == null) {
      coordinator = new OStructuralCoordinator(Executors.newSingleThreadExecutor(), opLog);
      OStructuralLoopBackDistributeDistributedMember loopBack = new OStructuralLoopBackDistributeDistributedMember(nodeName, submitContext, coordinator,
          executor);
      coordinator.join(loopBack);
      submitContext.setCoordinator(loopBack);
      executor.join(loopBack);
    }

  }

  public synchronized void setExternalCoordinator(OStructuralDistributedMember coordinator) {
    if (this.coordinator != null) {
      this.coordinator.close();
      this.coordinator = null;
    }
    submitContext.setCoordinator(coordinator);
    executor.join(coordinator);
  }

  public synchronized void close() {
    if (coordinator != null)
      coordinator.close();
    executor.close();
  }

}
