package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

public class OTransactionSecondPhaseHandler implements OResponseHandler {

  private final boolean success;
  private       int     responseCount = 0;

  public OTransactionSecondPhaseHandler(boolean success) {
    this.success = success;
  }

  @Override
  public boolean receive(ODistributedCoordinator coordinator, ORequestContext context, ODistributedMember member,
      ONodeResponse response) {
    responseCount++;
    if (responseCount >= context.getQuorum()) {
      if (success) {
        //coordinator
      }
    }
    return responseCount == context.getInvolvedMembers().size();
  }

  @Override
  public boolean timeout(ODistributedCoordinator coordinator, ORequestContext context) {
    return false;
  }
}
