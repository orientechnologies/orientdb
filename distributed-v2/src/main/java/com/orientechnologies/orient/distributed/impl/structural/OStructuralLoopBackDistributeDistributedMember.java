package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

public class OStructuralLoopBackDistributeDistributedMember extends OStructuralDistributedMember {
  private       OStructuralSubmitContext       submitContext;
  private       OStructuralCoordinator         coordinator;
  private final OStructuralDistributedExecutor executor;

  public OStructuralLoopBackDistributeDistributedMember(ONodeIdentity identity, OStructuralSubmitContext submitContext,
      OStructuralCoordinator coordinator, OStructuralDistributedExecutor executor) {
    super(identity, null);
    this.submitContext = submitContext;
    this.coordinator = coordinator;
    this.executor = executor;
  }

  public void sendRequest(OLogId id, OStructuralNodeRequest nodeRequest) {
    executor.receive(this, id, nodeRequest);
  }

  public void reply(OSessionOperationId operationId, OStructuralSubmitResponse response) {
    submitContext.receive(operationId, response);
  }

  public void sendResponse(OLogId opId, OStructuralNodeResponse response) {
    coordinator.receive(this, opId, response);
  }

  public void submit(OSessionOperationId operationId, OStructuralSubmitRequest request) {
    coordinator.submit(this, operationId, request);
  }

}
