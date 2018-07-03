package com.orientechnologies.orient.server.distributed.impl.coordinator;

public interface OResponseHandler {
  ORequestContext.Status receive(ODistributedCoordinator coordinator, ORequestContext context, ONodeResponse response,
      ORequestContext.Status status);
}
