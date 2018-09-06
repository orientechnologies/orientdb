package com.orientechnologies.orient.server.distributed.impl.coordinator;

import java.io.*;

public class OLogId {
  private long id;

  public OLogId(long id) {
    this.id = id;
  }

  public static void serialize(OLogId id, DataOutput output) throws IOException {
    output.writeLong(id.id);
  }

  public static OLogId deserialize(DataInput input) throws IOException {
    return new OLogId(input.readLong());
  }
}
