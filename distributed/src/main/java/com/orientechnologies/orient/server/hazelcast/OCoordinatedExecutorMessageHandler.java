package com.orientechnologies.orient.server.hazelcast;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODistributedCoordinator;
import com.orientechnologies.orient.core.db.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedExecutor;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedMember;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OSubmitContext;
import com.orientechnologies.orient.server.distributed.impl.coordinator.network.*;
import com.orientechnologies.orient.server.distributed.impl.metadata.ODistributedContext;

public class OCoordinatedExecutorMessageHandler implements OCoordinatedExecutor {
  private OrientDBDistributed distributed;

  public OCoordinatedExecutorMessageHandler(OrientDBDistributed distributed) {
    this.distributed = distributed;
  }

  private void waitOnline(String node, String database) {
    try {
      OHazelcastPlugin plugin = distributed.getPlugin();
      plugin.waitUntilNodeOnline(plugin.getLocalNodeName(), database);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void executeOperationRequest(OOperationRequest request) {
    waitOnline(request.getSenderNode(), request.getDatabase());
    ODistributedContext distributedContext = distributed.getDistributedContext(request.getDatabase());
    ODistributedExecutor executor = distributedContext.getExecutor();
    ODistributedMember member = executor.getMember(request.getSenderNode());
    executor.receive(member, request.getId(), request.getRequest());
  }

  @Override
  public void executeOperationResponse(OOperationResponse response) {
    waitOnline(response.getSenderNode(), response.getDatabase());
    ODistributedContext distributedContext = distributed.getDistributedContext(response.getDatabase());
    ODistributedCoordinator coordinator = distributedContext.getCoordinator();
    if (coordinator == null) {
      OLogManager.instance().error(this, "Received coordinator response on a node that is not a coordinator ignoring it", null);
    } else {
      ODistributedMember member = coordinator.getMember(response.getSenderNode());
      coordinator.receive(member, response.getId(), response.getResponse());
    }
  }

  @Override
  public void executeSubmitResponse(ONetworkSubmitResponse response) {
    waitOnline(response.getSenderNode(), response.getDatabase());
    ODistributedContext distributedContext = distributed.getDistributedContext(response.getDatabase());
    OSubmitContext context = distributedContext.getSubmitContext();
    context.receive(response.getOperationId(), response.getResponse());
  }

  @Override
  public void executeSubmitRequest(ONetworkSubmitRequest request) {
    waitOnline(request.getSenderNode(), request.getDatabase());
    ODistributedContext distributedContext = distributed.getDistributedContext(request.getDatabase());
    ODistributedCoordinator coordinator = distributedContext.getCoordinator();
    if (coordinator == null) {
      OLogManager.instance().error(this, "Received submit request on a node that is not a coordinator ignoring it", null);
    } else {
      ODistributedMember member = coordinator.getMember(request.getSenderNode());
      coordinator.submit(member, request.getOperationId(), request.getRequest());
    }
  }
}
