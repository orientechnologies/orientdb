package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.distributed.impl.coordinator.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface OStructuralNodeRequest extends OLogRequest {
  OStructuralNodeResponse execute(OOperationContext context);

  default OStructuralNodeResponse recover(OOperationContext executor) {
    return execute(executor);
  }

  void serialize(DataOutput output) throws IOException;

  void deserialize(DataInput input) throws IOException;

  int getRequestType();
}
