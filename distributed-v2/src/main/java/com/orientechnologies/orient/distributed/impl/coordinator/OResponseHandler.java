package com.orientechnologies.orient.distributed.impl.coordinator;

public interface OResponseHandler {
  boolean receive(ODistributedCoordinator coordinator, ORequestContext context, ODistributedMember member, ONodeResponse response);

  boolean timeout(ODistributedCoordinator coordinator, ORequestContext context);
}
