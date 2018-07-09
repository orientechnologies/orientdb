package com.orientechnologies.orient.server.distributed.impl.coordinator.mocktx;

import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

public class OSubmitTx implements OSubmitRequest {
  boolean firstPhase  = false;
  boolean secondPhase = false;

  @Override
  public void begin(ODistributedMember member, ODistributedCoordinator coordinator) {
    coordinator.sendOperation(this, new OPhase1Tx(), (coordinator1, context, response, status) -> {
      if (context.getResponses().size() >= context.getQuorum() && status == ORequestContext.Status.STARTED) {
        status = ORequestContext.Status.QUORUM_OK;
        firstPhase = true;
        coordinator1.sendOperation(this, new OPhase2Tx(), (coordinator2, context1, response1, status1) -> {
          if (context1.getResponses().size() >= context1.getQuorum() && status1 == ORequestContext.Status.STARTED) {
            status1 = ORequestContext.Status.QUORUM_OK;
            secondPhase = true;
            member.reply(new OSubmitResponse() {
            });
          }
          return status1;
        });
      }
      return status;
    });
  }

}
