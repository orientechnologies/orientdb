package com.orientechnologies.orient.distributed.impl.coordinator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface ONodeResponse {

  void serialize(DataOutput output) throws IOException;

  void deserialize(DataInput input) throws IOException;

  int getResponseType();
}
