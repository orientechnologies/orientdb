package com.orientechnologies.orient.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface OSubmitRequest {
  void begin(
      ONodeIdentity requester,
      OSessionOperationId operationId,
      ODistributedCoordinator coordinator);

  void serialize(DataOutput output) throws IOException;

  void deserialize(DataInput input) throws IOException;

  int getRequestType();
}
