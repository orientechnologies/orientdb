package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OInvalidSequentialAcceptResult implements OAcceptResult {

  @Override
  public boolean canRetry() {
    return true;
  }

  public static OInvalidSequentialAcceptResult fromNetwork(DataInput input) throws IOException {
    return new OInvalidSequentialAcceptResult();
  }

  @Override
  public void serialize(DataOutput out) throws IOException {}

  @Override
  public short getType() {
    return 1;
  }
}
