package com.orientechnologies.orient.server.distributed.impl.coordinator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

public class ODistributedCoordinator {

  private Executor                               requestExecutor;
  private OOperationLog                          operationLog;
  private ConcurrentMap<OLogId, ORequestContext> contexts = new ConcurrentHashMap<>();
  private OSender                                sender;

  public ODistributedCoordinator(Executor requestExecutor, OOperationLog operationLog, OSender sender) {
    this.requestExecutor = requestExecutor;
    this.operationLog = operationLog;
    this.sender = sender;
  }

  public void submit(OSubmitRequest request) {
    requestExecutor.execute(() -> {
      request.begin(this);
    });
  }

  public void sendAll(ONodeRequest request) {

  }

  public void receive(ONodeResponse response) {
    OLogId id = response.getOperationLogId();
    contexts.get(id).receive(response);
  }

  public OLogId log(ONodeRequest request) {
    return operationLog.log(request);
  }

  public ORequestContext logAndCreateContext(OSubmitRequest submitRequest, ONodeRequest nodeRequest) {
    OLogId id = log(nodeRequest);
    ORequestContext context = new ORequestContext(this, submitRequest, nodeRequest);
    contexts.put(id, context);
    return context;
  }

}
