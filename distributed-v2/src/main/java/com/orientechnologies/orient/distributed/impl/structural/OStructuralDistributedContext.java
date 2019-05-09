package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.OPersistentOperationalLogV1;
import com.orientechnologies.orient.distributed.impl.coordinator.OOperationLog;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.raft.OStructuralLeader;
import com.orientechnologies.orient.distributed.impl.structural.raft.OStructuralFollower;

import java.util.concurrent.Executors;

public class OStructuralDistributedContext {
  private OStructuralSubmitContext submitContext;
  private OOperationLog            opLog;
  private OrientDBDistributed      context;
  private OStructuralLeader        leader;
  private OStructuralFollower      follower;

  public OStructuralDistributedContext(OrientDBDistributed context) {
    this.context = context;
    initOpLog();
    submitContext = new OStructuralSubmitContextImpl();
    follower = new OStructuralFollower(Executors.newSingleThreadExecutor(), opLog, context);
    leader = null;
  }

  private void initOpLog() {
    this.opLog = OPersistentOperationalLogV1
        .newInstance("OSystem", context, (x) -> context.getCoordinateMessagesFactory().createStructuralOperationRequest(x));
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

  public synchronized void makeLeader(ONodeIdentity identity, OStructuralSharedConfiguration sharedConfiguration) {
    if (leader == null) {
      int quorum = sharedConfiguration.getQuorum();
      int timeout = 100;
      leader = new OStructuralLeader(Executors.newSingleThreadExecutor(), opLog, context, quorum, timeout);
    }
    OStructuralLoopBackDistributeDistributedMember loopbackLeader = new OStructuralLoopBackDistributeDistributedMember(identity,
        submitContext, leader, follower);
    leader.connected(loopbackLeader);
    this.getSubmitContext().setLeader(loopbackLeader);
  }

  public synchronized void setExternalLeader(OStructuralDistributedMember coordinator) {
    if (this.leader != null) {
      this.leader.close();
      this.leader = null;
    }
    this.getSubmitContext().setLeader(coordinator);
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
}
