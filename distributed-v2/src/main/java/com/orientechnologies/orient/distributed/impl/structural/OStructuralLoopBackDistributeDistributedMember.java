package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;
import com.orientechnologies.orient.distributed.impl.structural.raft.OStructuralFollower;
import com.orientechnologies.orient.distributed.impl.structural.raft.OStructuralLeader;

public class OStructuralLoopBackDistributeDistributedMember extends OStructuralDistributedMember {
  private       OStructuralSubmitContext submitContext;
  private       OStructuralLeader        leader;
  private final OStructuralFollower      follower;

  public OStructuralLoopBackDistributeDistributedMember(ONodeIdentity identity, OStructuralSubmitContext submitContext,
      OStructuralLeader leader, OStructuralFollower follower) {
    super(identity, null);
    this.submitContext = submitContext;
    this.leader = leader;
    this.follower = follower;
  }

  public void sendRequest(OLogId id, OStructuralNodeRequest nodeRequest) {
    //follower.receive(this, id, nodeRequest);
  }

  public void reply(OSessionOperationId operationId, OStructuralSubmitResponse response) {
    submitContext.receive(operationId, response);
  }

  public void sendResponse(OLogId opId, OStructuralNodeResponse response) {
    //leader.receive(this, opId, response);
  }

  public void submit(OSessionOperationId operationId, OStructuralSubmitRequest request) {
    leader.receiveSubmit(getIdentity(), operationId, request);
  }

  @Override
  public void confirm(OLogId id) {
  }

  @Override
  public void ack(OLogId logId) {
  }

  public void propagate(OLogId id, ORaftOperation operation) {
  }

}
