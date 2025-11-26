package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OAlreadyEnstablishedTopologyState implements OAcceptResult {

  public static OAlreadyEnstablishedTopologyState fromNetwork(DataInput input) throws IOException {
    return new OAlreadyEnstablishedTopologyState();
  }

  @Override
  public void serialize(DataOutput out) throws IOException {}

  @Override
  public short getType() {
    return 3;
  }

  @Override
  public String toString() {
    return "Already Enstablished Topology State";
  }
}
