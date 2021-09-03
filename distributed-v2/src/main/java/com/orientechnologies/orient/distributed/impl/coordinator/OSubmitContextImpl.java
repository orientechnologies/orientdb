package com.orientechnologies.orient.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class OSubmitContextImpl implements OSubmitContext {

  private Map<OSessionOperationId, CompletableFuture<OSubmitResponse>> operations = new HashMap<>();
  private volatile ONodeIdentity coordinator;
  private OrientDBDistributed orientDBDistributed;
  private String database;

  public OSubmitContextImpl(OrientDBDistributed orientDBDistributed, String database) {
    this.orientDBDistributed = orientDBDistributed;
    this.database = database;
  }

  @Override
  public synchronized Future<OSubmitResponse> send(
      OSessionOperationId operationId, OSubmitRequest request) {
    while (coordinator == null) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    CompletableFuture<OSubmitResponse> value = new CompletableFuture<>();
    operations.put(operationId, value);
    orientDBDistributed.getNetworkManager().submit(coordinator, database, operationId, request);
    return value;
  }

  @Override
  public synchronized void receive(OSessionOperationId requestId, OSubmitResponse response) {
    CompletableFuture<OSubmitResponse> future = operations.remove(requestId);
    future.complete(response);
  }

  @Override
  public ONodeIdentity getCoordinator() {
    return coordinator;
  }

  @Override
  public synchronized void setCoordinator(ONodeIdentity coordinator) {
    this.coordinator = coordinator;
    notifyAll();
  }
}
