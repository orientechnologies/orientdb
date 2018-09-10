package com.orientechnologies.orient.server.distributed.impl.coordinator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface OSubmitRequest {
  void begin(ODistributedMember member, ODistributedCoordinator coordinator);

  void serialize(DataOutput output) throws IOException;

  void deserialize(DataInput input) throws IOException;

  int getRequestType();
}
