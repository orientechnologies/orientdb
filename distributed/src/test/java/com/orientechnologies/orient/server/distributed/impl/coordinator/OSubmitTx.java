package com.orientechnologies.orient.server.distributed.impl.coordinator;

public class OSubmitTx implements OSubmitRequest {
  @Override
  public void begin(ODistributedCoordinator coordinator) {
    coordinator.sendOperation(this, new OPhase1Tx(), (coordinator1, context, response) -> {
      if (context.getResponses().size() >= context.getQuorum()) {
        coordinator1.sendOperation(this, new OPhase2Tx(), (coordinator2, context1, response1) -> {
          if (context1.getResponses().size() >= context1.getQuorum()) {
            coordinator2.reply(new OSubmitResponse() {
            });
          }
        });
      }
    });
  }

}
