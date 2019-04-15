package com.orientechnologies.orient.distributed.impl.structural.operations;

import com.orientechnologies.orient.distributed.impl.structural.*;

public class ODropDatabaseResponseHandler implements OStructuralResponseHandler {
  private OStructuralSubmitId id;

  public ODropDatabaseResponseHandler(OStructuralSubmitId id) {
    this.id = id;
  }

  @Override
  public boolean receive(OCoordinationContext coordinator, OStructuralRequestContext context, OStructuralDistributedMember member,
      OStructuralNodeResponse response) {
    if (context.getInvolvedMembers().size() == context.getResponses().size()) {
      coordinator.reply(id, new ODropDatabaseSubmitResponse());
      return true;
    }
    return false;
  }

  @Override
  public boolean timeout(OCoordinationContext coordinator, OStructuralRequestContext context) {
    return false;
  }
}
