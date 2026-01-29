package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public record ONotQuorumOneMerge() implements OAcceptResult {

  public static ONotQuorumOneMerge fromNetwork(DataInput input) {
    return new ONotQuorumOneMerge();
  }

  @Override
  public void serialize(DataOutput out) throws IOException {}

  @Override
  public short getType() {
    return 11;
  }
}
