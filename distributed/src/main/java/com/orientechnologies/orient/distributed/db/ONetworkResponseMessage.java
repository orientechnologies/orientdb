package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.db.ONetworkMessage;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ONetworkResponseMessage implements ONetworkMessage {

  private final OrientDBDistributed ctx;
  private final ODistributedResponse response;

  public ONetworkResponseMessage(OrientDBDistributed ctx) {
    this.ctx = ctx;
    this.response = new ODistributedResponse();
  }

  @Override
  public void execute() {
    ctx.getMessageService().dispatchResponseToThread(response);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    response.fromStream(input);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    throw new UnsupportedOperationException("not needed for now");
  }
}
