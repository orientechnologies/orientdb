package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

public class ODropDatabaseResponseHandler implements OStructuralResponseHandler {
  private OStructuralDistributedMember sender;
  private OSessionOperationId          operationId;

  public ODropDatabaseResponseHandler(OStructuralDistributedMember sender, OSessionOperationId operationId) {
    this.sender = sender;
    this.operationId = operationId;
  }

  @Override
  public boolean receive(OStructuralCoordinator coordinator, OStructuralRequestContext context, OStructuralDistributedMember member,
      OStructuralNodeResponse response) {
    if (context.getInvolvedMembers().size() == context.getResponses().size()) {
      coordinator.reply(sender, operationId, new ODropDatabaseSubmitResponse());
      return true;
    }
    return false;
  }

  @Override
  public boolean timeout(OStructuralCoordinator coordinator, OStructuralRequestContext context) {
    return false;
  }
}
