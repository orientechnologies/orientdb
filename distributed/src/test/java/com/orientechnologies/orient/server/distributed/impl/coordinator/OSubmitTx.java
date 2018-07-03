package com.orientechnologies.orient.server.distributed.impl.coordinator;

public class OSubmitTx implements OSubmitRequest {
  @Override
  public void begin(ODistributedCoordinator coordinator) {
    coordinator.sendOperation(this, new OPhase1Tx(), (coordinator1, context, response, status) -> {
      if (context.getResponses().size() >= context.getQuorum() && status == ORequestContext.Status.STARTED) {
        status = ORequestContext.Status.QUORUM_OK;
        coordinator1.sendOperation(this, new OPhase2Tx(), (coordinator2, context1, response1, status1) -> {
          if (context1.getResponses().size() >= context1.getQuorum() && status1 == ORequestContext.Status.STARTED) {
            status1 = ORequestContext.Status.QUORUM_OK;
            coordinator2.reply(new OSubmitResponse() {
            });
          }
          return status1;
        });
      }
      return status;
    });
  }

}
