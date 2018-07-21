package com.orientechnologies.orient.server.distributed.impl.coordinator;

public interface OResponseHandler {
  void receive(ODistributedCoordinator coordinator, ORequestContext context, ODistributedMember member, ONodeResponse response);

  void timeout(ODistributedCoordinator coordinator, ORequestContext context);
}
