package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedMember;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.OOperationLog;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralDistributedMember;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class OStructuralSlave implements AutoCloseable {
  private OOperationLog               operationLog;
  private ExecutorService             executor;
  private OrientDBDistributed         orientDB;
  private Map<OLogId, ORaftOperation> pending = new HashMap<>();

  public OStructuralSlave(ExecutorService executor, OOperationLog operationLog, OrientDBDistributed orientDB) {
    this.operationLog = operationLog;
    this.executor = executor;
    this.orientDB = orientDB;
  }

  public void log(OStructuralDistributedMember member, OLogId logId, ORaftOperation operation) {
    executor.execute(() -> {
      //TODO: this should check that the operation is in order and in case request the copy.
      operationLog.logReceived(logId, operation);
      pending.put(logId, operation);
      member.ack(logId);
    });
  }

  public void confirm(OLogId logId) {
    executor.execute(() -> {
      //TODO: The pending should be a queue we cannot really apply things in random order
      ORaftOperation op = pending.get(logId);
      op.apply(orientDB);
    });
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
    return null;
  }

  public void recover(ORaftOperation request) {
    executor.execute(() -> {
      request.apply(orientDB);
    });
  }
}
