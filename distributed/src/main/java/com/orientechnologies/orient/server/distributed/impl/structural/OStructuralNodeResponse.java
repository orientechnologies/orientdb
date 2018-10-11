package com.orientechnologies.orient.server.distributed.impl.structural;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface OStructuralNodeResponse {

  void serialize(DataOutput output) throws IOException;

  void deserialize(DataInput input) throws IOException;

  int getResponseType();
}
