package com.orientechnologies.orient.distributed.impl.metadata;

import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.OClusterPositionAllocatorDatabase;
import com.orientechnologies.orient.distributed.impl.OPersistentOperationalLogV1;
import com.orientechnologies.orient.distributed.impl.coordinator.*;
import com.orientechnologies.orient.distributed.impl.coordinator.lock.ODistributedLockManagerImpl;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class ODistributedContext {

  private ODistributedExecutor              executor;
  private OSubmitContext                    submitContext;
  private ODistributedCoordinator           coordinator;
  private OClusterPositionAllocatorDatabase allocator;
  private OrientDBInternal                  context;
  private String                            databaseName;
  private OOperationLog                     opLog;

  public ODistributedContext(OStorage storage, OrientDBDistributed context) {
    this.context = context;
    this.databaseName = storage.getName();
    initOpLog();
    executor = new ODistributedExecutor(Executors.newSingleThreadExecutor(), opLog, context, storage.getName());
    submitContext = new OSubmitContextImpl();
    coordinator = null;

  }

  private void initOpLog() {
    this.opLog = OPersistentOperationalLogV1.newInstance(databaseName, context,
        (x) -> ((OrientDBDistributed) context).getCoordinateMessagesFactory().createOperationRequest(x));
//    this.opLog = new OIncrementOperationalLog();
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
      allocator = new OClusterPositionAllocatorDatabase(context);
      coordinator = new ODistributedCoordinator(Executors.newSingleThreadExecutor(), opLog, new ODistributedLockManagerImpl(),
          allocator);
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
      allocator = null;
    }
    submitContext.setCoordinator(lockManager);
    executor.join(lockManager);
  }

  public synchronized void close() {
    if (coordinator != null) {
      coordinator.close();
    }
    executor.close();
  }

  public synchronized void reload() {
    if (allocator != null) {
      allocator.reload();
    }
  }
}
