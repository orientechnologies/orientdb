package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.db.ONetworkMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.OStructuralMessage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ONetworkMessageStructural implements ONetworkMessage {

  private OrientDBDistributed context;
  private OStructuralMessage message;

  public ONetworkMessageStructural(OrientDBDistributed ctx) {
    this.context = ctx;
  }

  public ONetworkMessageStructural(OrientDBDistributed ctx, OStructuralMessage message) {
    this.context = ctx;
    this.message = message;
  }

  @Override
  public void execute() {
    this.context.receiveMessage(message);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    message = OStructuralMessage.readNetwork(input);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    message.writeNetwork(out);
  }
}
