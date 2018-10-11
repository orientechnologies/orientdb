package com.orientechnologies.orient.server.distributed.impl.structural;

import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface OStructuralSubmitRequest {
  void begin(OStructuralDistributedMember member, OSessionOperationId operationId, OStructuralCoordinator coordinator);

  void serialize(DataOutput output) throws IOException;

  void deserialize(DataInput input) throws IOException;

  int getRequestType();

}
