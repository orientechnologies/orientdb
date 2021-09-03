package com.orientechnologies.orient.distributed.impl.coordinator.mocktx;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.ORequestContext;
import com.orientechnologies.orient.distributed.impl.coordinator.OResponseHandler;

public class FirstPhaseHandler implements OResponseHandler {
  private OSubmitTx submitTx;
  private final ONodeIdentity member;
  private boolean done;

  public FirstPhaseHandler(OSubmitTx submitTx, ONodeIdentity member) {
    this.submitTx = submitTx;
    this.member = member;
  }

  @Override
  public boolean receive(
      ODistributedCoordinator coordinator1,
      ORequestContext context,
      ONodeIdentity member,
      ONodeResponse response) {
    if (context.getResponses().size() >= context.getQuorum() && !done) {
      done = true;
      submitTx.firstPhase = true;
      coordinator1.sendOperation(
          submitTx, new OPhase2Tx(), new SecondPhaseResponseHandler(submitTx, this.member));
    }
    return context.getResponses().size() == context.getInvolvedMembers().size();
  }

  @Override
  public boolean timeout(ODistributedCoordinator coordinator, ORequestContext context) {
    return true;
  }
}
