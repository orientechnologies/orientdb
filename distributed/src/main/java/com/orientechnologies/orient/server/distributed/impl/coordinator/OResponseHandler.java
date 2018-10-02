package com.orientechnologies.orient.server.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.ODistributedCoordinator;

public interface OResponseHandler {
  boolean receive(ODistributedCoordinator coordinator, ORequestContext context, ODistributedMember member, ONodeResponse response);

  boolean timeout(ODistributedCoordinator coordinator, ORequestContext context);
}
