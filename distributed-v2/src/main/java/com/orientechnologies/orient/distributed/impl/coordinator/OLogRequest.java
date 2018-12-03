package com.orientechnologies.orient.distributed.impl.coordinator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface OLogRequest {
  void serialize(DataOutput output) throws IOException;

  int getRequestType();

  void deserialize(DataInput input) throws IOException;
}
