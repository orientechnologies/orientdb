package com.orientechnologies.orient.server.distributed.impl.coordinator;

import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OSessionOperationId;

public interface OSubmitContext {

  void send(OSessionOperationId requestId, OSubmitRequest response);

  void receive(OSessionOperationId requestId, OSubmitResponse response);

  ODistributedMember getCoordinator();

  void setCoordinator(ODistributedMember m);
}
