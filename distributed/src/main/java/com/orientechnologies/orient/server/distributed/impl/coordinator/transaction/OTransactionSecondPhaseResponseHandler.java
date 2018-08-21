package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

public class OTransactionSecondPhaseResponseHandler implements OResponseHandler {

  private       OTransactionSubmit request;
  private       ODistributedMember requester;
  private final boolean            success;
  private       int                responseCount = 0;

  public OTransactionSecondPhaseResponseHandler(boolean success, OTransactionSubmit request, ODistributedMember requester) {
    this.success = success;
    this.request = request;
    this.requester = requester;
  }

  @Override
  public boolean receive(ODistributedCoordinator coordinator, ORequestContext context, ODistributedMember member,
      ONodeResponse response) {
    responseCount++;
    if (responseCount >= context.getQuorum()) {
      if (success) {
        coordinator.reply(requester, new OTransactionResponse());
      }
    }
    return responseCount == context.getInvolvedMembers().size();
  }

  @Override
  public boolean timeout(ODistributedCoordinator coordinator, ORequestContext context) {
    return false;
  }
}
