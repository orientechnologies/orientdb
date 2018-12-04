package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface OStructuralSubmitRequest {
  void begin(OStructuralDistributedMember sender, OSessionOperationId operationId, OStructuralCoordinator coordinator,
      OrientDBDistributed context);

  void serialize(DataOutput output) throws IOException;

  void deserialize(DataInput input) throws IOException;

  int getRequestType();

}
