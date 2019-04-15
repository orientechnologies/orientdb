package com.orientechnologies.orient.distributed.impl.structural.operations;

import com.orientechnologies.orient.distributed.impl.structural.*;

public class OCreateDatabaseResponseHandler implements OStructuralResponseHandler {

  private OCreateDatabaseSubmitRequest request;
  private OStructuralSubmitId          id;
  private String                       database;

  public OCreateDatabaseResponseHandler(OCreateDatabaseSubmitRequest request, OStructuralSubmitId id, String database) {
    this.request = request;
    this.id = id;
    this.database = database;
  }

  @Override
  public boolean receive(OCoordinationContext coordinator, OStructuralRequestContext context, OStructuralDistributedMember member,
      OStructuralNodeResponse response) {
    if (context.getInvolvedMembers().size() == context.getResponses().size()) {
      if (context.getResponses().values().stream().filter((f) -> !((OCreateDatabaseOperationResponse) f).isCreated()).count()
          == 0) {
        coordinator
            .sendOperation(new OCreateDatabaseFinalizeRequest(true, database), new OCreateDatabaseFinalizeResponseHandler());
        coordinator.reply(id, new OCreateDatabaseSubmitResponse(true, ""));
      } else {
        coordinator
            .sendOperation(new OCreateDatabaseFinalizeRequest(false, database), new OCreateDatabaseFinalizeResponseHandler());
        coordinator.reply(id, new OCreateDatabaseSubmitResponse(true, "Database Not Created"));
      }
    }

    return false;
  }

  @Override
  public boolean timeout(OCoordinationContext coordinator, OStructuralRequestContext context) {
    return false;
  }
}
