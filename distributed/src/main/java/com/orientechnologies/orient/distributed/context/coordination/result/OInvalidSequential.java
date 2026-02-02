package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public record OInvalidSequential(long expected, long received) implements OAcceptResult {

  @Override
  public boolean consensusRetry() {
    return true;
  }

  @Override
  public boolean executeRetry() {
    return true;
  }

  public static OInvalidSequential fromNetwork(DataInput input) throws IOException {
    long expected = input.readLong();
    long received = input.readLong();
    return new OInvalidSequential(expected, received);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    out.writeLong(expected);
    out.writeLong(received);
  }

  @Override
  public short getType() {
    return 1;
  }

  @Override
  public String toString() {
    return "Invalid Sequential [expected=" + expected + ", received=" + received + "]";
  }
}
