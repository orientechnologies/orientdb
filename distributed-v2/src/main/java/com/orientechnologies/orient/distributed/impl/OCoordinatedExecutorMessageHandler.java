package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedExecutor;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedMember;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitContext;
import com.orientechnologies.orient.distributed.impl.coordinator.network.*;
import com.orientechnologies.orient.distributed.impl.metadata.ODistributedContext;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralDistributedContext;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralDistributedMember;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitContext;
import com.orientechnologies.orient.distributed.impl.structural.raft.OStructuralLeader;
import com.orientechnologies.orient.distributed.impl.structural.raft.OStructuralFollower;

public class OCoordinatedExecutorMessageHandler implements OCoordinatedExecutor {
  private OrientDBDistributed distributed;

  public OCoordinatedExecutorMessageHandler(OrientDBDistributed distributed) {
    this.distributed = distributed;
  }

  private void checkDatabaseReady(String database) {
    distributed.checkDatabaseReady(database);
  }

  @Override
  public void executeOperationRequest(ONodeIdentity sender, OOperationRequest request) {
    checkDatabaseReady(request.getDatabase());
    ODistributedContext distributedContext = distributed.getDistributedContext(request.getDatabase());
    ODistributedExecutor executor = distributedContext.getExecutor();
    ODistributedMember member = executor.getMember(sender);
    executor.receive(member, request.getId(), request.getRequest());
  }

  @Override
  public void executeOperationResponse(ONodeIdentity sender, OOperationResponse response) {
    checkDatabaseReady(response.getDatabase());
    ODistributedContext distributedContext = distributed.getDistributedContext(response.getDatabase());
    ODistributedCoordinator coordinator = distributedContext.getCoordinator();
    if (coordinator == null) {
      OLogManager.instance().error(this, "Received coordinator response on a node that is not a coordinator ignoring it", null);
    } else {
      ODistributedMember member = coordinator.getMember(sender);
      coordinator.receive(member, response.getId(), response.getResponse());
    }
  }

  @Override
  public void executeSubmitResponse(ONodeIdentity sender, ONetworkSubmitResponse response) {
    checkDatabaseReady(response.getDatabase());
    ODistributedContext distributedContext = distributed.getDistributedContext(response.getDatabase());
    OSubmitContext context = distributedContext.getSubmitContext();
    context.receive(response.getOperationId(), response.getResponse());
  }

  @Override
  public void executeSubmitRequest(ONodeIdentity sender, ONetworkSubmitRequest request) {
    checkDatabaseReady(request.getDatabase());
    ODistributedContext distributedContext = distributed.getDistributedContext(request.getDatabase());
    ODistributedCoordinator coordinator = distributedContext.getCoordinator();
    if (coordinator == null) {
      OLogManager.instance().error(this, "Received submit request on a node that is not a coordinator ignoring it", null);
    } else {
      ODistributedMember member = coordinator.getMember(sender);
      coordinator.submit(member, request.getOperationId(), request.getRequest());
    }
  }

  @Override
  public void executeStructuralSubmitRequest(ONodeIdentity sender, ONetworkStructuralSubmitRequest request) {
    OStructuralDistributedContext distributedContext = distributed.getStructuralDistributedContext();
    distributedContext.execute(sender, request.getOperationId(), request.getRequest());
  }

  @Override
  public void executeStructuralSubmitResponse(ONodeIdentity sender, ONetworkStructuralSubmitResponse response) {
    OStructuralDistributedContext distributedContext = distributed.getStructuralDistributedContext();
    OStructuralSubmitContext context = distributedContext.getSubmitContext();
    context.receive(response.getOperationId(), response.getResponse());
  }

  @Override
  public void executePropagate(ONodeIdentity sender, ONetworkPropagate propagate) {
    OStructuralDistributedContext distributedContext = distributed.getStructuralDistributedContext();
    OStructuralFollower slave = distributedContext.getFollower();
    OStructuralDistributedMember member = slave.getMember(sender);
    slave.log(member, propagate.getId(), propagate.getOperation());
  }

  @Override
  public void executeConfirm(ONodeIdentity sender, ONetworkConfirm confirm) {
    OStructuralDistributedContext distributedContext = distributed.getStructuralDistributedContext();
    OStructuralFollower slave = distributedContext.getFollower();
    slave.confirm(confirm.getId());
  }

  @Override
  public void executeAck(ONodeIdentity sender, ONetworkAck ack) {
    OStructuralDistributedContext distributedContext = distributed.getStructuralDistributedContext();
    OStructuralLeader master = distributedContext.getLeader();
    if (master == null) {
      OLogManager.instance().error(this, "Received coordinator response on a node that is not a coordinator ignoring it", null);
    } else {
      master.receiveAck(sender, ack.getLogId());
    }
  }
}
