package com.orientechnologies.orient.distributed.impl.metadata;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.OClusterPositionAllocatorDatabase;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedExecutor;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitContext;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitContextImpl;
import com.orientechnologies.orient.distributed.impl.coordinator.lock.ODistributedLockManagerImpl;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.log.OOperationLog;
import com.orientechnologies.orient.distributed.impl.log.OPersistentOperationalLogV1;
import java.util.Set;
import java.util.concurrent.Executors;

public class ODistributedContext {

  private ODistributedExecutor executor;
  private OSubmitContext submitContext;
  private ODistributedCoordinator coordinator;
  private OClusterPositionAllocatorDatabase allocator;
  private OrientDBDistributed context;
  private String databaseName;
  private OOperationLog opLog;

  public ODistributedContext(OStorage storage, OrientDBDistributed context) {
    this.context = context;
    this.databaseName = storage.getName();
    initOpLog();
    executor =
        new ODistributedExecutor(
            Executors.newSingleThreadExecutor(),
            opLog,
            context,
            context.getNetworkManager(),
            storage.getName());
    submitContext = new OSubmitContextImpl(context, storage.getName());
    coordinator = null;
  }

  private void initOpLog() {
    this.opLog =
        OPersistentOperationalLogV1.newInstance(
            databaseName, context, (x) -> OCoordinateMessagesFactory.createOperationRequest(x));
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

  public synchronized void makeCoordinator(
      ONodeIdentity leader,
      String database,
      OLogId leaderLastValid,
      Set<ONodeIdentity> activeNodes) {
    if (coordinator == null) {
      // TODO: Make sure that this leader kind of sync with the leader valid oplog id
      allocator = new OClusterPositionAllocatorDatabase(context.getSharedContext(database));
      coordinator =
          new ODistributedCoordinator(
              Executors.newSingleThreadExecutor(),
              opLog,
              new ODistributedLockManagerImpl(),
              allocator,
              context.getNetworkManager(),
              databaseName);
      for (ONodeIdentity node : activeNodes) {
        coordinator.join(node);
      }
      submitContext.setCoordinator(context.getNodeIdentity());
    }
  }

  public synchronized void setExternalCoordinator(
      ONodeIdentity lockManager, OLogId leaderLastValid) {
    if (coordinator != null) {
      coordinator.close();
      coordinator = null;
      allocator = null;
    }
    submitContext.setCoordinator(lockManager);
    executor.notifyLastValidLog(lockManager, leaderLastValid);
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

  public synchronized void connected(ONodeIdentity nodeIdentity) {
    if (coordinator != null) {
      coordinator.join(nodeIdentity);
    }
  }

  public synchronized void disconnected(ONodeIdentity nodeIdentity) {
    if (coordinator != null) {
      coordinator.leave(nodeIdentity);
    }
  }

  public OOperationLog getOpLog() {
    return opLog;
  }
}
