package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

public class OCreateDatabaseResponseHandler implements OStructuralResponseHandler {

  private OCreateDatabaseSubmitRequest request;
  private OStructuralDistributedMember sender;
  private OSessionOperationId          sessionOperationId;
  private String                        database;

  public OCreateDatabaseResponseHandler(OCreateDatabaseSubmitRequest request, OStructuralDistributedMember sender,
      OSessionOperationId sessionOperationId, String database) {
    this.request = request;
    this.sender = sender;
    this.sessionOperationId = sessionOperationId;
    this.database = database;
  }

  @Override
  public boolean receive(OStructuralCoordinator coordinator, OStructuralRequestContext context, OStructuralDistributedMember member,
      OStructuralNodeResponse response) {
    if (context.getInvolvedMembers().size() == context.getResponses().size()) {
      if (context.getResponses().values().stream().filter((f) -> !((OCreateDatabaseOperationResponse) f).isCreated()).count()
          == 0) {
        coordinator.sendOperation(null, new OCreateDatabaseFinalizeRequest(true, database),
            new OCreateDatabaseFinalizeResponseHandler());
        coordinator.reply(sender, sessionOperationId, new OCreateDatabaseSubmitResponse(true, ""));
      } else {
        coordinator.sendOperation(null, new OCreateDatabaseFinalizeRequest(false, database),
            new OCreateDatabaseFinalizeResponseHandler());
        coordinator.reply(sender, sessionOperationId, new OCreateDatabaseSubmitResponse(true, "Database Not Created"));
      }
    }

    return false;
  }

  @Override
  public boolean timeout(OStructuralCoordinator coordinator, OStructuralRequestContext context) {
    return false;
  }
}
