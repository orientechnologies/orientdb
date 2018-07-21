package com.orientechnologies.orient.server.distributed.impl.coordinator.mocktx;

import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

public class FirstPhaseHandler implements OResponseHandler {
  private       OSubmitTx          submitTx;
  private final ODistributedMember member;
  private       boolean            done;

  public FirstPhaseHandler(OSubmitTx submitTx, ODistributedMember member) {
    this.submitTx = submitTx;
    this.member = member;
  }

  @Override
  public void receive(ODistributedCoordinator coordinator1, ORequestContext context, ODistributedMember member, ONodeResponse response) {
    if (context.getResponses().size() >= context.getQuorum() && !done) {
      done = true;
      submitTx.firstPhase = true;
      coordinator1.sendOperation(submitTx, new OPhase2Tx(), new SecondPhaseResponseHandler(submitTx, this.member));
    }
  }

  @Override
  public void timeout(ODistributedCoordinator coordinator, ORequestContext context) {

  }

}
