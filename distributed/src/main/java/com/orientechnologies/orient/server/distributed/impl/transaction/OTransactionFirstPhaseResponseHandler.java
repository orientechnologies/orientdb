package com.orientechnologies.orient.server.distributed.impl.transaction;

import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedMember;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ORequestContext;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OResponseHandler;

public class OTransactionFirstPhaseResponseHandler implements OResponseHandler {
  @Override
  public boolean receive(ODistributedCoordinator coordinator, ORequestContext context, ODistributedMember member,
      ONodeResponse response) {
    return false;
  }

  @Override
  public boolean timeout(ODistributedCoordinator coordinator, ORequestContext context) {
    return false;
  }
}
