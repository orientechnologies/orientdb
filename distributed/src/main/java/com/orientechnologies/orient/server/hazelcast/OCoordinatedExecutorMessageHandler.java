package com.orientechnologies.orient.server.hazelcast;

import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedExecutor;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedMember;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OSubmitContext;
import com.orientechnologies.orient.server.distributed.impl.coordinator.network.*;

public class OCoordinatedExecutorMessageHandler implements OCoordinatedExecutor {
  private ODistributedCoordinator coordinator;
  private ODistributedExecutor    executor;
  private ODistributedMember      member;
  private OSubmitContext          context;

  public OCoordinatedExecutorMessageHandler(ODistributedCoordinator coordinator, ODistributedExecutor executor,
      ODistributedMember member, OSubmitContext context) {
    this.coordinator = coordinator;
    this.executor = executor;
    this.member = member;
    this.context = context;
  }

  @Override
  public void executeOperationRequest(OOperationRequest request) {
    executor.receive(member, request.getId(), request.getRequest());
  }

  @Override
  public void executeOperationResponse(OOperationResponse response) {
    coordinator.receive(member, response.getId(), response.getResponse());
  }

  @Override
  public void executeSubmitResponse(ONetworkSubmitResponse response) {
    context.receive(response.getResponse());
  }

  @Override
  public void executeSubmitRequest(ONetworkSubmitRequest request) {
    coordinator.submit(null, request.getRequest());
  }
}
