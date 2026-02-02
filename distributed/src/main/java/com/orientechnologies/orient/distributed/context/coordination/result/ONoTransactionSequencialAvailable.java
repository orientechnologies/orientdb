package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public record ONoTransactionSequencialAvailable() implements OAcceptResult {

  @Override
  public boolean executeRetry() {
    return true;
  }

  @Override
  public boolean consensusRetry() {
    return true;
  }

  static ONoTransactionSequencialAvailable fromNetwork(DataInput input) {
    return new ONoTransactionSequencialAvailable();
  }

  @Override
  public void serialize(DataOutput out) throws IOException {}

  @Override
  public short getType() {
    return 9;
  }

  @Override
  public String toString() {
    return "No Transaction Sequential Available";
  }
}
