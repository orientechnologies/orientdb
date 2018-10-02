package com.orientechnologies.orient.server.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.ODistributedCoordinator;

public interface ODistributedCoordinatorInternal extends ODistributedCoordinator {

  void executeOperation(Runnable runnable);

  void finish(OLogId requestId);
}
