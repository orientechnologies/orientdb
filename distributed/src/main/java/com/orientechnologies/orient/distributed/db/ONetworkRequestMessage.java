package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.db.ONetworkMessage;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ONetworkRequestMessage implements ONetworkMessage {

  private final OrientDBDistributed ctx;
  private final ODistributedRequest req;

  public ONetworkRequestMessage(OrientDBDistributed ctx) {
    this.ctx = ctx;
    req = new ODistributedRequest(ctx.getTaskFactoryManager());
  }

  @Override
  public void execute() {
    ctx.executeDistributedRequest(req);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    req.fromStream(input);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    throw new UnsupportedOperationException("not needed so far");
  }
}
