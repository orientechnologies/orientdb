package com.orientechnologies.orient.server.distributed.impl.coordinator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

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

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    OLogId oLogId = (OLogId) o;
    return id == oLogId.id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  public long getId() {
    return id;
  }
}
