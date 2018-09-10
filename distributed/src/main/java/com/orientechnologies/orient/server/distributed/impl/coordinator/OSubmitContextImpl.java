package com.orientechnologies.orient.server.distributed.impl.coordinator;

import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class OSubmitContextImpl implements OSubmitContext {

  private Map<OSessionOperationId, CompletableFuture<OSubmitResponse>> operations = new HashMap<>();
  private ODistributedMember                                           coordinator;

  @Override
  public synchronized void send(OSessionOperationId operationId, OSubmitRequest request) {
    CompletableFuture<OSubmitResponse> value = new CompletableFuture<>();
    operations.put(operationId, value);
    coordinator.submit(operationId, request);
  }

  @Override
  public synchronized void receive(OSessionOperationId requestId, OSubmitResponse response) {
    CompletableFuture<OSubmitResponse> future = operations.remove(requestId);
    future.complete(response);
  }

  @Override
  public ODistributedMember getCoordinator() {
    return coordinator;
  }

  @Override
  public void setCoordinator(ODistributedMember m) {
    this.coordinator = m;
  }
}
