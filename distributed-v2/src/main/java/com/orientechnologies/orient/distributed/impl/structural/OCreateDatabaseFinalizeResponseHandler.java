package com.orientechnologies.orient.distributed.impl.structural;

public class OCreateDatabaseFinalizeResponseHandler implements OStructuralResponseHandler {
  @Override
  public boolean receive(OStructuralCoordinator coordinator, OStructuralRequestContext context, OStructuralDistributedMember member,
      OStructuralNodeResponse response) {

    return context.getResponses().size() == context.getInvolvedMembers().size();
  }

  @Override
  public boolean timeout(OStructuralCoordinator coordinator, OStructuralRequestContext context) {
    return false;
  }
}
