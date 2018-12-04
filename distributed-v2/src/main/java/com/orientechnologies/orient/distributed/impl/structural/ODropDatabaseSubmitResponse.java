package com.orientechnologies.orient.distributed.impl.structural;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ODropDatabaseSubmitResponse implements OStructuralSubmitResponse {
  @Override
  public void serialize(DataOutput output) throws IOException {

  }

  @Override
  public void deserialize(DataInput input) throws IOException {

  }

  @Override
  public int getResponseType() {
    return 0;
  }
}
