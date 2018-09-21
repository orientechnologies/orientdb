package com.orientechnologies.orient.server.distributed.impl.metadata;

import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.OrientDBDistributed;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.server.distributed.impl.OClusterPositionAllocatorDatabase;
import com.orientechnologies.orient.server.distributed.impl.OIncrementOperationalLog;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class ODistributedContext {

  private Map<OSessionOperationId, OTransactionContext> transactions;
  private ODistributedExecutor                          executor;
  private OSubmitContext                                submitContext;
  private ODistributedCoordinator                       coordinator;
  private OrientDBInternal                              context;
  private String                                        databaseName;

  public ODistributedContext(OStorage storage, OrientDBDistributed context) {
    transactions = new ConcurrentHashMap<>();
    executor = new ODistributedExecutor(Executors.newSingleThreadExecutor(), new OIncrementOperationalLog(), context,
        storage.getName());
    submitContext = new OSubmitContextImpl();
    coordinator = null;
    this.context = context;
    this.databaseName = storage.getName();
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

  public ODistributedExecutor getExecutor() {
    return executor;
  }

  public OSubmitContext getSubmitContext() {
    return submitContext;
  }

  public synchronized ODistributedCoordinator getCoordinator() {
    return coordinator;
  }

  public synchronized void makeCoordinator(String nodeName, OSharedContext context) {
    if (coordinator == null) {
      coordinator = new ODistributedCoordinator(Executors.newSingleThreadExecutor(), new OIncrementOperationalLog(),
          new ODistributedLockManagerImpl(0), new OClusterPositionAllocatorDatabase(context));
      OLoopBackDistributeMember loopBack = new OLoopBackDistributeMember(nodeName, databaseName, submitContext, coordinator,
          executor);
      coordinator.join(loopBack);
      submitContext.setCoordinator(loopBack);
      executor.join(loopBack);
    }

  }

  public synchronized void setExternalCoordinator(ODistributedMember lockManager) {
    if (coordinator != null) {
      coordinator.close();
      coordinator = null;
    }
    submitContext.setCoordinator(lockManager);
    executor.join(lockManager);
  }

  public synchronized void close() {
    if (coordinator != null)
      coordinator.close();
    executor.close();
  }
}
