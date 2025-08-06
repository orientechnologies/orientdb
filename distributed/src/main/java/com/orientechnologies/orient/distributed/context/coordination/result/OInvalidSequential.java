package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OInvalidSequential implements OAcceptResult {

  @Override
  public boolean canRetry() {
    return true;
  }

  public static OInvalidSequential fromNetwork(DataInput input) throws IOException {
    return new OInvalidSequential();
  }

  @Override
  public void serialize(DataOutput out) throws IOException {}

  @Override
  public short getType() {
    return 1;
  }
}
