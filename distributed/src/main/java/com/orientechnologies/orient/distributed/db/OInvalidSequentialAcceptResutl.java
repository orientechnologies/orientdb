package com.orientechnologies.orient.distributed.db;

import java.io.DataOutput;

public class OInvalidSequentialAcceptResutl implements OAcceptResult {

  @Override
  public boolean canRetry() {
    return true;
  }

  @Override
  public void writeNetwork(DataOutput out) {}
}
