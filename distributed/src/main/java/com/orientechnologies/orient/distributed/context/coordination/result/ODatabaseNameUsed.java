package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ODatabaseNameUsed implements OAcceptResult {

  @Override
  public void serialize(DataOutput out) throws IOException {}

  @Override
  public short getType() {
    return 4;
  }

  public static ODatabaseNameUsed fromNetwork(DataInput input) {
    return new ODatabaseNameUsed();
  }

  @Override
  public String toString() {
    return "Database Name Used";
  }
}
