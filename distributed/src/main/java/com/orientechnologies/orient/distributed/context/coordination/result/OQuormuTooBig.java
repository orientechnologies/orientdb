package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public record OQuormuTooBig() implements OAcceptResult {

  public static OQuormuTooSmall fromNetwork(DataInput input) throws IOException {
    return new OQuormuTooSmall();
  }

  @Override
  public void serialize(DataOutput out) throws IOException {}

  @Override
  public short getType() {
    return 16;
  }
}
