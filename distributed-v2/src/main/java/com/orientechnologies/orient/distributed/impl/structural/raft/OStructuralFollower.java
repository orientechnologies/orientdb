package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.*;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralDistributedMember;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class OStructuralFollower implements AutoCloseable {
  private OOperationLog               operationLog;
  private ExecutorService             executor;
  private OrientDBDistributed         orientDB;
  private Map<OLogId, ORaftOperation> pending = new HashMap<>();
  private OSessionOperationIdWaiter   waiter  = new OSessionOperationIdWaiter();

  public OStructuralFollower(ExecutorService executor, OOperationLog operationLog, OrientDBDistributed orientDB) {
    this.operationLog = operationLog;
    this.executor = executor;
    this.orientDB = orientDB;
  }

  public void log(OStructuralDistributedMember member, OLogId logId, ORaftOperation operation) {
    executor.execute(() -> {
      if (operationLog.logReceived(logId, operation)) {
        pending.put(logId, operation);
        member.ack(logId);
      } else {
        resyncOplog();
      }
    });
  }

  private void resyncOplog() {
    orientDB.nodeSyncRequest(operationLog.lastPersistentLog());
  }

  public void confirm(OLogId logId) {
    executor.execute(() -> {
      //TODO: The pending should be a queue we cannot really apply things in random order
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
    Iterator<OOperationLogEntry> res = operationLog.iterate(lastStateId.getId(), confirmedId.getId());
    while (res.hasNext()) {
      OOperationLogEntry entry = res.next();
      if (!pending.containsKey(entry.getLogId())) {
        pending.put(entry.getLogId(), (ORaftOperation) entry.getRequest());
      }
      confirm(confirmedId);
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

  public OStructuralDistributedMember getMember(ONodeIdentity senderNode) {
    return new OStructuralDistributedMember(senderNode, orientDB.getNetworkManager().getChannel(senderNode));
  }

  public void recover(ORaftOperation request) {
    executor.execute(() -> {
      request.apply(orientDB);
    });
  }
}
