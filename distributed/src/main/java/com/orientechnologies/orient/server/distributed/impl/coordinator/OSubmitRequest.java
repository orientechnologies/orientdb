package com.orientechnologies.orient.server.distributed.impl.coordinator;

public interface OSubmitRequest {
  void begin(ODistributedMember member, ODistributedCoordinator coordinator);
}
