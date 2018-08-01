package com.orientechnologies.orient.server.distributed.impl.transaction;

import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

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
