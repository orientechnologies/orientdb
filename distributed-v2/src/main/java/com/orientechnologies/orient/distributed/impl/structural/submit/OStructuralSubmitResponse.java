package com.orientechnologies.orient.distributed.impl.structural.submit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface OStructuralSubmitResponse {
  void serialize(DataOutput output) throws IOException;

  void deserialize(DataInput input) throws IOException;

  int getResponseType();
}
