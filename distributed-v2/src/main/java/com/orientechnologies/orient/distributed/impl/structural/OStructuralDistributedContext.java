package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.OPersistentOperationalLogV1;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedChannel;
import com.orientechnologies.orient.distributed.impl.coordinator.OOperationLog;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.operations.OCreateDatabaseSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.operations.OCreateDatabaseSubmitResponse;
import com.orientechnologies.orient.distributed.impl.structural.raft.OStructuralLeader;
import com.orientechnologies.orient.distributed.impl.structural.raft.OStructuralFollower;

import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class OStructuralDistributedContext {
  private OStructuralSubmitContext submitContext;
  private OOperationLog            opLog;
  private OrientDBDistributed      context;
  private OStructuralLeader        leader;
  private OStructuralFollower      follower;
  private OSessionOperationId      last;

  public OStructuralDistributedContext(OrientDBDistributed context) {
    this.context = context;
    initOpLog();
    submitContext = new OStructuralSubmitContextImpl();
    follower = new OStructuralFollower(Executors.newSingleThreadExecutor(), opLog, context);
    leader = null;
  }

  private void initOpLog() {
    this.opLog = OPersistentOperationalLogV1
        .newInstance("OSystem", context, (x) -> OCoordinateMessagesFactory.createRaftOperation(x));
  }

  public OStructuralSubmitContext getSubmitContext() {
    return submitContext;
  }

  public OOperationLog getOpLog() {
    return opLog;
  }

  public OStructuralLeader getLeader() {
    return leader;
  }

  public OStructuralFollower getFollower() {
    return follower;
  }

  public synchronized void makeLeader(ONodeIdentity identity) {
    if (leader == null) {
      leader = new OStructuralLeader(Executors.newSingleThreadExecutor(), opLog, context);
    }
    OLoopBackDistributedChannel loopback = new OLoopBackDistributedChannel(identity, submitContext, leader, follower);
    leader.connected(identity, loopback);
    this.getSubmitContext().setLeader(loopback);
  }

  public synchronized void setExternalLeader(ODistributedChannel leader) {
    if (this.leader != null) {
      this.leader.close();
      this.leader = null;
    }
    this.getSubmitContext().setLeader(leader);
  }

  public synchronized void close() {
    if (leader != null)
      leader.close();
    follower.close();
  }

  public void execute(ONodeIdentity senderNode, OSessionOperationId operationId, OStructuralSubmitRequest request) {
    if (leader != null) {
      leader.receiveSubmit(senderNode, operationId, request);
    }
  }

  private synchronized OSessionOperationId nextOpId() {
    if (last == null) {
      last = new OSessionOperationId(context.getNodeIdentity().getId());
    } else {
      last = last.next();
    }
    return last;
  }

  private synchronized OSessionOperationId getLastOpId() {
    return last;
  }

  public Future<OStructuralSubmitResponse> forward(OStructuralSubmitRequest request) {
    return getSubmitContext().send(nextOpId(), request);
  }

  public void waitApplyLastRequest() {
    OSessionOperationId lastOpId = getLastOpId();
    if (lastOpId != null) {
      follower.waitForExecution(lastOpId);
    }
  }

  public OStructuralSubmitResponse forwardAndWait(OStructuralSubmitRequest request) {
    return getSubmitContext().sendAndWait(nextOpId(), request);
  }

}
