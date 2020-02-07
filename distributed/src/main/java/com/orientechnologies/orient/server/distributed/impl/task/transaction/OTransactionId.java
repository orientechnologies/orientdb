package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class OTransactionId {
  private int  position;
  private long sequence;

  public OTransactionId(int position, long sequence) {
    this.position = position;
    this.sequence = sequence;
  }

  public int getPosition() {
    return position;
  }

  public long getSequence() {
    return sequence;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    OTransactionId that = (OTransactionId) o;
    return position == that.position && sequence == that.sequence;
  }

  @Override
  public int hashCode() {
    return Objects.hash(position, sequence);
  }

  public static OTransactionId read(DataInput input) throws IOException {
    int position = input.readInt();
    long sequence = input.readLong();
    return new OTransactionId(position, sequence);
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(position);
    out.writeLong(sequence);
  }
}
