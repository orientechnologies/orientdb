package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public record ODisconnectedNode() implements OAcceptResult {

  public static ODisconnectedNode fromNetwork(DataInput input) {
    return new ODisconnectedNode();
  }

  @Override
  public void serialize(DataOutput out) throws IOException {}

  @Override
  public short getType() {
    return 12;
  }
}
