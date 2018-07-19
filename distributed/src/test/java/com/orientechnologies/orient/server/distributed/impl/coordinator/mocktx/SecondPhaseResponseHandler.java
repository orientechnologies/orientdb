package com.orientechnologies.orient.server.distributed.impl.coordinator.mocktx;

import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

public class SecondPhaseResponseHandler implements OResponseHandler {
  private final OSubmitTx          submitTx;
  private final ODistributedMember member;
  boolean done = false;

  public SecondPhaseResponseHandler(OSubmitTx submitTx, ODistributedMember member) {
    this.member = member;
    this.submitTx = submitTx;
  }

  @Override
  public void receive(ODistributedCoordinator coordinator, ORequestContext context1, ONodeResponse response) {
    if (context1.getResponses().size() >= context1.getQuorum() && !done) {
      done = true;
      submitTx.secondPhase = true;
      member.reply(new OSubmitResponse() {
      });
    }
  }

  @Override
  public void timeout(ODistributedCoordinator coordinator, ORequestContext context) {

  }
}
