package com.orientechnologies.orient.distributed.impl.coordinator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class OLogId implements Comparable<OLogId> {
  private long id;
  private long term;

  public OLogId(long id, long term) {
    this.id = id;
    this.term = term;
  }

  public static void serialize(OLogId id, DataOutput output) throws IOException {
    if (id == null) {
      output.writeLong(-1);
      output.writeLong(-1);
    } else {
      output.writeLong(id.id);
      output.writeLong(id.term);
    }
  }

  public static OLogId deserialize(DataInput input) throws IOException {
    long val = input.readLong();
    long term = input.readLong();
    if (val == -1) {
      return null;
    } else {
      return new OLogId(term, term);
    }
  }


  public long getId() {
    return id;
  }

  @Override
  public int compareTo(OLogId o) {
    return ((Long) this.id).compareTo(o.id);
  }


  public long getTerm() {
    return term;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OLogId oLogId = (OLogId) o;
    return id == oLogId.id
            && term == oLogId.term;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, term);
  }
}
