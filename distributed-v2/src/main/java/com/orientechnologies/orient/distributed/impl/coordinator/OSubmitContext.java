package com.orientechnologies.orient.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import java.util.concurrent.Future;

public interface OSubmitContext {

  Future<OSubmitResponse> send(OSessionOperationId requestId, OSubmitRequest response);

  void receive(OSessionOperationId requestId, OSubmitResponse response);

  ONodeIdentity getCoordinator();

  void setCoordinator(ONodeIdentity coordinator);
}
