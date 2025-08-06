package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OAlreadyEnstablishedTopologyState implements OAcceptResult {

  @Override
  public boolean canRetry() {
    return false;
  }

  public static OAlreadyEnstablishedTopologyState fromNetwork(DataInput input) throws IOException {
    return new OAlreadyEnstablishedTopologyState();
  }

  @Override
  public void serialize(DataOutput out) throws IOException {}

  @Override
  public short getType() {
    return 3;
  }
}
