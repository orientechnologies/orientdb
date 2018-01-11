package com.orientechnologies.orient.server.distributed.operation;

import java.io.DataInput;
import java.io.DataOutput;

public interface NodeOperationResponse {

  void write(DataOutput out);

  void read(DataInput in);

}
