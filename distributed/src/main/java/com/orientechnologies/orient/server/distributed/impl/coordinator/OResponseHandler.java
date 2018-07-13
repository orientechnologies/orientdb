package com.orientechnologies.orient.server.distributed.impl.coordinator;

public interface OResponseHandler {
  void receive(ODistributedCoordinator coordinator, ORequestContext context, ONodeResponse response);
}
