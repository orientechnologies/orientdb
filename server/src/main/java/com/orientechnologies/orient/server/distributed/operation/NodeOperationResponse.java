package com.orientechnologies.orient.server.distributed.operation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface NodeOperationResponse {

  void write(DataOutput out) throws IOException;

  void read(DataInput in) throws IOException;

  default boolean isOk() {
    return true;
  }
}
