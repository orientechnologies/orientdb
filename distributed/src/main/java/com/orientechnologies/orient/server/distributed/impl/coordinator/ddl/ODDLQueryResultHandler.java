package com.orientechnologies.orient.server.distributed.impl.coordinator.ddl;

import com.orientechnologies.orient.server.distributed.impl.coordinator.*;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OSessionOperationId;

public class ODDLQueryResultHandler implements OResponseHandler {

  private ODistributedMember  sender;
  private OSessionOperationId operationId;

  public ODDLQueryResultHandler(ODistributedMember sender, OSessionOperationId operationId) {
    this.sender = sender;
    this.operationId = operationId;
  }

  @Override
  public boolean receive(ODistributedCoordinator coordinator, ORequestContext context, ODistributedMember member,
      ONodeResponse response) {
    if (context.getInvolvedMembers().size() == context.getResponses().size()) {
      coordinator.reply(sender, operationId, new ODDLQuerySubmitResponse());
    }
    return false;
  }

  @Override
  public boolean timeout(ODistributedCoordinator coordinator, ORequestContext context) {
    return false;
  }
}
