package com.orientechnologies.orient.distributed.context.coordination.dbs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public enum ONodeRole {
  Main,
  Replica;

  public void writeNetwork(DataOutput output) throws IOException {
    output.writeInt(this.ordinal());
  }

  public static ONodeRole readNetwork(DataInput input) throws IOException {
    int ord = input.readInt();
    return ONodeRole.values()[ord];
  }
}
