package com.orientechnologies.orient.server.distributed.impl.coordinator.mocktx;

import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedMember;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OSubmitRequest;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OSubmitTx implements OSubmitRequest {

  public boolean firstPhase;
  public boolean secondPhase;

  @Override
  public void begin(ODistributedMember requester, OSessionOperationId operationId, ODistributedCoordinator coordinator) {
    coordinator.sendOperation(this, new OPhase1Tx(), new FirstPhaseHandler(this, requester));
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
