package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedMember;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.coordinator.OOperationLog;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralDistributedMember;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class OStructuralSlave {
  private OOperationLog               operationLog;
  private ExecutorService             executor;
  private OrientDBDistributed         orientDB;
  private Map<OLogId, ORaftOperation> pending = new HashMap<>();

  private void log(OStructuralDistributedMember member, OLogId logId, ORaftOperation operation) {
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
}
