package com.orientechnologies.orient.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;

public interface OResponseHandler {
  boolean receive(
      ODistributedCoordinator coordinator,
      ORequestContext context,
      ONodeIdentity member,
      ONodeResponse response);

  boolean timeout(ODistributedCoordinator coordinator, ORequestContext context);
}
