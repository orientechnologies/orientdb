package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralCoordinator;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralDistributedMember;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitRequest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OJoinStructuralCoordination implements OStructuralSubmitRequest {
  @Override
  public void begin(OStructuralDistributedMember sender, OSessionOperationId operationId, OStructuralCoordinator coordinator,
      OrientDBDistributed context) {
    //coordinator
  }

  @Override
  public void serialize(DataOutput output) throws IOException {

  }

  @Override
  public void deserialize(DataInput input) throws IOException {

  }

  @Override
  public int getRequestType() {
    return 0;
  }
}
