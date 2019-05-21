package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.*;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.raft.OFullConfiguration;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;
import com.orientechnologies.orient.distributed.impl.structural.raft.OStructuralFollower;
import com.orientechnologies.orient.distributed.impl.structural.raft.OStructuralLeader;

public class OLoopBackDistributedChannel implements ODistributedChannel {
  private       OStructuralSubmitContext submitContext;
  private       OStructuralLeader        leader;
  private final OStructuralFollower      follower;
  private       ONodeIdentity            currentNode;

  public OLoopBackDistributedChannel(ONodeIdentity currentNode, OStructuralSubmitContext submitContext, OStructuralLeader leader,
      OStructuralFollower follower) {
    this.currentNode = currentNode;
    this.submitContext = submitContext;
    this.leader = leader;
    this.follower = follower;
  }

  @Override
  public void submit(String database, OSessionOperationId operationId, OSubmitRequest request) {

  }

  @Override
  public void reply(String database, OSessionOperationId operationId, OSubmitResponse response) {

  }

  @Override
  public void sendRequest(String database, OLogId id, ONodeRequest nodeRequest) {

  }

  @Override
  public void sendResponse(String database, OLogId id, ONodeResponse nodeResponse) {

  }

  @Override
  public void sendResponse(OLogId opId, OStructuralNodeResponse response) {

  }

  @Override
  public void sendRequest(OLogId id, OStructuralNodeRequest request) {

  }

  @Override
  public void reply(OSessionOperationId operationId, OStructuralSubmitResponse response) {
    submitContext.receive(operationId, response);
  }

  @Override
  public void submit(OSessionOperationId operationId, OStructuralSubmitRequest request) {
    leader.receiveSubmit(currentNode, operationId, request);
  }

  @Override
  public void propagate(OLogId id, ORaftOperation operation) {

  }

  @Override
  public void confirm(OLogId id) {

  }

  @Override
  public void ack(OLogId logId) {

  }

  @Override
  public void send(OFullConfiguration fullConfiguration) {

  }
}
