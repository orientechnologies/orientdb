package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.log.OOperationLog;
import com.orientechnologies.orient.distributed.impl.log.OOperationLogEntry;
import com.orientechnologies.orient.distributed.impl.log.OOplogIterator;
import com.orientechnologies.orient.distributed.network.ODistributedNetwork;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OStructuralFollower implements AutoCloseable {
  private final ODistributedNetwork network;
  private OOperationLog operationLog;
  private ExecutorService executor;
  private OrientDBDistributed orientDB;
  private Map<OLogId, ORaftOperation> pending = new HashMap<>();
  private OSessionOperationIdWaiter waiter = new OSessionOperationIdWaiter();

  public OStructuralFollower(
      OOperationLog operationLog, ODistributedNetwork network, OrientDBDistributed orientDB) {
    this.operationLog = operationLog;
    this.executor = Executors.newSingleThreadExecutor();
    this.orientDB = orientDB;
    this.network = network;
  }

  public void log(ONodeIdentity leader, OLogId logId, ORaftOperation operation) {
    executor.execute(
        () -> {
          if (operationLog.logReceived(logId, operation)) {
            pending.put(logId, operation);
            network.ack(leader, logId);
          } else {
            resyncOplog();
          }
        });
  }

  private void resyncOplog() {
    orientDB.nodeSyncRequest(operationLog.lastPersistentLog());
  }

  public void confirm(OLogId logId) {
    executor.execute(
        () -> {
          // TODO: The pending should be a queue we cannot really apply things in random order
          OLogId lastStateId = orientDB.getStructuralConfiguration().getLastUpdateId();
          if (lastStateId == null || logId.getId() - 1 == lastStateId.getId()) {
            ORaftOperation op = pending.get(logId);
            if (op != null) {
              op.apply(orientDB);
              op.getRequesterSequential().ifPresent(this::notifyDone);
            }
          } else if (logId.getId() > lastStateId.getId()) {
            enqueueFrom(lastStateId, logId);
          }
        });
  }

  private void enqueueFrom(OLogId lastStateId, OLogId confirmedId) {
    OOplogIterator res = operationLog.iterate(lastStateId.getId(), confirmedId.getId());
    try {
      while (res.hasNext()) {
        OOperationLogEntry entry = res.next();
        if (!pending.containsKey(entry.getLogId())) {
          pending.put(entry.getLogId(), (ORaftOperation) entry.getRequest());
        }
        confirm(confirmedId);
      }
    } finally {
      res.close();
    }
  }

  private void notifyDone(OSessionOperationId requesterSequential) {
    if (requesterSequential.getNodeId().equals(orientDB.getNodeIdentity().getId())) {
      waiter.notify(requesterSequential);
    }
  }

  public void waitForExecution(OSessionOperationId id) {
    waiter.waitIfNeeded(id);
  }

  @Override
  public void close() {
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void recover(ORaftOperation request) {
    executor.execute(
        () -> {
          request.apply(orientDB);
        });
  }

  public void ping(ONodeIdentity leader, OLogId leaderLastValid) {
    // TODO: verify leader
    executor.execute(
        () -> {
          OLogId lastLogId = operationLog.lastPersistentLog();
          if (lastLogId == null || leaderLastValid.getId() > lastLogId.getId()) {
            resyncOplog();
          }
        });
  }
}
