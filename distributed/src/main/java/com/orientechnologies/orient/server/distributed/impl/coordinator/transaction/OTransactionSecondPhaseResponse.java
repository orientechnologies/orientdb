package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.server.distributed.impl.coordinator.ONodeResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.orientechnologies.orient.server.distributed.impl.coordinator.OCoordinateMessagesFactory.TRANSACTION_SECOND_PHASE_RESPONSE;

public class OTransactionSecondPhaseResponse implements ONodeResponse {
  private boolean success;

  public OTransactionSecondPhaseResponse(boolean success) {
    this.success = success;
  }

  public OTransactionSecondPhaseResponse() {
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeBoolean(success);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    success = input.readBoolean();
  }

  @Override
  public int getResponseType() {
    return TRANSACTION_SECOND_PHASE_RESPONSE;
  }
}
