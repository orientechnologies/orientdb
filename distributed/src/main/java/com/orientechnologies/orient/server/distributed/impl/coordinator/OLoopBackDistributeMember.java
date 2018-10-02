package com.orientechnologies.orient.server.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.ODistributedCoordinator;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OSessionOperationId;

public class OLoopBackDistributeMember extends ODistributedMember {
  private       OSubmitContext          submitContext;
  private       ODistributedCoordinator coordinator;
  private final ODistributedExecutor    executor;

  public OLoopBackDistributeMember(String name, String database, OSubmitContext submitContext, ODistributedCoordinator coordinator,
      ODistributedExecutor executor) {
    super(name, database, null);
    this.submitContext = submitContext;
    this.coordinator = coordinator;
    this.executor = executor;
  }

  public void sendRequest(OLogId id, ONodeRequest nodeRequest) {
    executor.receive(this, id, nodeRequest);
  }

  public void reply(OSessionOperationId operationId, OSubmitResponse response) {
    submitContext.receive(operationId, response);
  }

  public void sendResponse(OLogId opId, ONodeResponse response) {
    coordinator.receive(this, opId, response);
  }

  public void submit(OSessionOperationId operationId, OSubmitRequest request) {
    coordinator.submit(this, operationId, request);
  }
}
