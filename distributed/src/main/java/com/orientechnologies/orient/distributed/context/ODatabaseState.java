package com.orientechnologies.orient.distributed.context;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public enum ODatabaseState {
  Exists,
  Online,
  Offline;

  public static ODatabaseState readNetwork(DataInput input) throws IOException {
    short s = input.readShort();
    return ODatabaseState.values()[s];
  }

  public void writeNetwork(DataOutput out) throws IOException {
    // TODO: make sure this is network compatible
    out.writeShort(this.ordinal());
  }
}
