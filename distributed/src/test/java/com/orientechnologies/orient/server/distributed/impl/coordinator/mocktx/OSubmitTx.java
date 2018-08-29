package com.orientechnologies.orient.server.distributed.impl.coordinator.mocktx;

import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

public class OSubmitTx implements OSubmitRequest {

  public boolean firstPhase;
  public boolean secondPhase;

  @Override
  public void begin(ODistributedMember member, ODistributedCoordinator coordinator) {
    coordinator.sendOperation(this, new OPhase1Tx(), new FirstPhaseHandler(this, member));
  }

}
