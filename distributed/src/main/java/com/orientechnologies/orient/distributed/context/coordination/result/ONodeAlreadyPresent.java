package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ONodeAlreadyPresent implements OAcceptResult {

  @Override
  public void serialize(DataOutput out) throws IOException {}

  @Override
  public short getType() {
    return 10;
  }

  public static ONodeAlreadyPresent fromNetwork(DataInput input) {
    return new ONodeAlreadyPresent();
  }
}
