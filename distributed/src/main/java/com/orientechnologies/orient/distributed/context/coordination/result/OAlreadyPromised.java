package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public record OAlreadyPromised() implements OAcceptResult {

  @Override
  public boolean consensusRetry() {
    return true;
  }

  @Override
  public void serialize(DataOutput out) throws IOException {}

  @Override
  public short getType() {
    return 6;
  }

  public static OAlreadyPromised fromNetwork(DataInput input) {
    return new OAlreadyPromised();
  }

  @Override
  public String toString() {
    return "Already Promised";
  }
}
