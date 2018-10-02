package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.server.distributed.impl.coordinator.*;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OSessionOperationId;

public interface ODistributedCoordinator {

  void submit(ODistributedMember member, OSessionOperationId operationId, OSubmitRequest request);

  void reply(ODistributedMember member, OSessionOperationId operationId, OSubmitResponse response);

  ORequestContext sendOperation(OSubmitRequest submitRequest, ONodeRequest nodeRequest, OResponseHandler handler);

  void receive(ODistributedMember member, OLogId relativeRequest, ONodeResponse response);

  default ODistributedLockManager getLockManager() {
    throw new UnsupportedOperationException();
  }

  default OClusterPositionAllocator getAllocator() {
    throw new UnsupportedOperationException();
  }

  ODistributedMember getMember(String node);
}
