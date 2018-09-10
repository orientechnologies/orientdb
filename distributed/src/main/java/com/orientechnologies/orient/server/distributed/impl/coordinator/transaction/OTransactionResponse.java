package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.server.distributed.impl.coordinator.OSubmitResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.orientechnologies.orient.server.distributed.impl.coordinator.OCoordinateMessagesFactory.TRANSACTION_SUBMIT_RESPONSE;

public class OTransactionResponse implements OSubmitResponse {
  @Override
  public void serialize(DataOutput output) throws IOException {

  }

  @Override
  public void deserialize(DataInput input) throws IOException {

  }

  @Override
  public int getResponseType() {
    return TRANSACTION_SUBMIT_RESPONSE;
  }
}
