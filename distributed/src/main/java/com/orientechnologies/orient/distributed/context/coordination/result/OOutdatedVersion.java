package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OOutdatedVersion implements OAcceptResult {

  private long proposed;
  private long current;

  public OOutdatedVersion(long current, long proposed) {
    this.current = current;
    this.proposed = proposed;
  }

  @Override
  public boolean executeRetry() {
    return true;
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    out.writeLong(current);
    out.writeLong(proposed);
  }

  @Override
  public short getType() {
    return 8;
  }

  public static OOutdatedVersion fromNetwork(DataInput input) throws IOException {
    long current = input.readLong();
    long proposed = input.readLong();
    return new OOutdatedVersion(current, proposed);
  }

  @Override
  public String toString() {
    return "Outdated Version";
  }
}
