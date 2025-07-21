package com.orientechnologies.orient.distributed.context.coordination.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OInvalidSequentialAcceptResutl implements OAcceptResult {

  @Override
  public boolean canRetry() {
    return true;
  }

  public static OInvalidSequentialAcceptResutl fromNetwork(DataInput input) throws IOException {
    return new OInvalidSequentialAcceptResutl();
  }

  @Override
  public void serialize(DataOutput out) throws IOException {}

  @Override
  public short getType() {
    return 1;
  }
}
