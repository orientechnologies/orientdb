package com.orientechnologies.orient.server.distributed.impl.structural;

import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.util.concurrent.Future;

public interface OStructuralSubmitContext {

  Future<OStructuralSubmitResponse> send(OSessionOperationId requestId, OStructuralSubmitRequest response);

  void receive(OSessionOperationId requestId, OStructuralSubmitResponse response);

  OStructuralDistributedMember getCoordinator();

  void setCoordinator(OStructuralDistributedMember m);
}
