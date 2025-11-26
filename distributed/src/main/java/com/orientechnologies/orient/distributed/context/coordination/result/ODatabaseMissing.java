package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ODatabaseMissing implements OAcceptResult {

  @Override
  public void serialize(DataOutput out) throws IOException {}

  @Override
  public short getType() {
    return 7;
  }

  public static ODatabaseMissing fromNetwork(DataInput input) {
    return new ODatabaseMissing();
  }

  @Override
  public String toString() {
    return "Database Missing";
  }
}
