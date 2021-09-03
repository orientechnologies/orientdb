package com.orientechnologies.orient.distributed.impl.log;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class OLogId implements Comparable<OLogId> {
  private long previousIdTerm;
  private long id;
  private long term;

  public OLogId(long id, long term, long previousIdTerm) {
    this.id = id;
    this.term = term;
    this.previousIdTerm = previousIdTerm;
  }

  public static void serialize(OLogId id, DataOutput output) throws IOException {
    if (id == null) {
      output.writeLong(-1);
      output.writeLong(-1);
      output.writeLong(-1);
    } else {
      output.writeLong(id.id);
      output.writeLong(id.term);
      output.writeLong(id.previousIdTerm);
    }
  }

  public static OLogId deserialize(DataInput input) throws IOException {
    long val = input.readLong();
    long term = input.readLong();
    long previousIdTerm = input.readLong();
    if (val == -1) {
      return null;
    } else {
      return new OLogId(val, term, previousIdTerm);
    }
  }

  public static void serializeOptional(Optional<OLogId> id, DataOutput output) throws IOException {
    serialize(id.orElse(null), output);
  }

  public static Optional<OLogId> deserializeOptional(DataInput input) throws IOException {
    return Optional.ofNullable(deserialize(input));
  }

  public long getId() {
    return id;
  }

  @Override
  public int compareTo(OLogId o) {
    if (this.getTerm() == o.getTerm()) {
      return ((Long) this.id).compareTo(o.id);
    } else {
      return ((Long) this.getTerm()).compareTo(o.getTerm());
    }
  }

  public long getTerm() {
    return term;
  }

  public long getPreviousIdTerm() {
    return previousIdTerm;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OLogId oLogId = (OLogId) o;
    return previousIdTerm == oLogId.previousIdTerm && id == oLogId.id && term == oLogId.term;
  }

  @Override
  public int hashCode() {
    return Objects.hash(previousIdTerm, id, term);
  }
}
