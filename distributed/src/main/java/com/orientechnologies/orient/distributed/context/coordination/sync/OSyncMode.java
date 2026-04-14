package com.orientechnologies.orient.distributed.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public enum OSyncMode {
  StandardBackup,
  IncrementalBackup,
  Delta,
  ;

  public void writeNetwork(DataOutput out) throws IOException {
    out.writeByte(this.ordinal());
  }

  public static OSyncMode fromNetwork(DataInput input) throws IOException {
    byte b = input.readByte();
    return OSyncMode.values()[b];
  }
}
