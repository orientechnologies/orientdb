package com.orientechnologies.orient.server.hazelcast;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseImpl;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedExecutor;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedMember;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OSubmitContext;
import com.orientechnologies.orient.server.distributed.impl.coordinator.network.*;

public class OCoordinatedExecutorMessageHandler implements OCoordinatedExecutor {
  private ODistributedServerManager distributed;

  public OCoordinatedExecutorMessageHandler(ODistributedServerManager distributed) {
    this.distributed = distributed;
  }

  @Override
  public void executeOperationRequest(OOperationRequest request) {
    ODistributedDatabase db = distributed.getMessageService().getDatabase(request.getDatabase());
    ODistributedExecutor executor = ((ODistributedDatabaseImpl) db).getExecutor();
    ODistributedMember member = executor.getMember(request.getSourceNode());
    executor.receive(member, request.getId(), request.getRequest());
  }

  @Override
  public void executeOperationResponse(OOperationResponse response) {
    ODistributedDatabase db = distributed.getMessageService().getDatabase(response.getDatabase());
    ODistributedCoordinator coordinator = ((ODistributedDatabaseImpl) db).getCoordinator();
    if (coordinator == null) {
      OLogManager.instance().error(this, "Received coordinator response on a node that is not a coordinator ignoring it", null);
    } else {
      ODistributedMember member = coordinator.getMember(response.getSenderNode());
      coordinator.receive(member, response.getId(), response.getResponse());
    }
  }

  @Override
  public void executeSubmitResponse(ONetworkSubmitResponse response) {
    ODistributedDatabase db = distributed.getMessageService().getDatabase(response.getDatabase());
    OSubmitContext context = ((ODistributedDatabaseImpl) db).getContext();
    context.receive(response.getOperationId(), response.getResponse());
  }

  @Override
  public void executeSubmitRequest(ONetworkSubmitRequest request) {
    ODistributedDatabase db = distributed.getMessageService().getDatabase(request.getDatabase());
    ODistributedCoordinator coordinator = ((ODistributedDatabaseImpl) db).getCoordinator();
    if (coordinator == null) {
      OLogManager.instance().error(this, "Received submit request on a node that is not a coordinator ignoring it", null);
    } else {
      ODistributedMember member = coordinator.getMember(request.getSenderNode());
      coordinator.submit(member, request.getOperationId(), request.getRequest());
    }
  }
}
