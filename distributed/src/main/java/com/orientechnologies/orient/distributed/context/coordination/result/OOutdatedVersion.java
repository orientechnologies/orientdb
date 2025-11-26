package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OOutdatedVersion implements OAcceptResult {

  @Override
  public boolean executeRetry() {
    return true;
  }

  @Override
  public void serialize(DataOutput out) throws IOException {}

  @Override
  public short getType() {
    return 8;
  }

  public static OOutdatedVersion fromNetwork(DataInput input) throws IOException {
    return new OOutdatedVersion();
  }

  @Override
  public String toString() {
    return "Outdated Version";
  }
}
