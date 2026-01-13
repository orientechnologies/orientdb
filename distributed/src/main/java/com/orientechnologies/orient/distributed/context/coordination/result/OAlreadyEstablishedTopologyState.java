package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OAlreadyEstablishedTopologyState implements OAcceptResult {

  public static OAlreadyEstablishedTopologyState fromNetwork(DataInput input) throws IOException {
    return new OAlreadyEstablishedTopologyState();
  }

  @Override
  public void serialize(DataOutput out) throws IOException {}

  @Override
  public short getType() {
    return 3;
  }

  @Override
  public String toString() {
    return "Already Established Topology State";
  }
}
