package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataOutput;
import java.io.IOException;

public class OAlreadyPromised implements OAcceptResult {

  @Override
  public boolean canRetry() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public short getType() {
    // TODO Auto-generated method stub
    return 0;
  }
}
