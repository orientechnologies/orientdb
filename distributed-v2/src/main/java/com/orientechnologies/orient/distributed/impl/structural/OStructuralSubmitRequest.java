package com.orientechnologies.orient.distributed.impl.structural;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface OStructuralSubmitRequest {
  void begin(OStructuralSubmitId id, OCoordinationContext coordinator);

  void serialize(DataOutput output) throws IOException;

  void deserialize(DataInput input) throws IOException;

  int getRequestType();

}
