package com.orientechnologies.orient.server.distributed.impl.coordinator.mocktx;

import com.orientechnologies.orient.core.db.ODistributedCoordinator;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OSubmitTx implements OSubmitRequest {

  public boolean firstPhase;
  public boolean secondPhase;

  @Override
  public void begin(ODistributedMember member, OSessionOperationId operationId, ODistributedCoordinator coordinator) {
    coordinator.sendOperation(this, new OPhase1Tx(), new FirstPhaseHandler(this, member));
  }

  @Override
  public void serialize(DataOutput output) throws IOException {

  }

  @Override
  public void deserialize(DataInput input) throws IOException {

  }

  @Override
  public int getRequestType() {
    return 0;
  }

}
