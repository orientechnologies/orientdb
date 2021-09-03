package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class OStructuralSubmitContextImpl implements OStructuralSubmitContext {

  private Map<OSessionOperationId, CompletableFuture<OStructuralSubmitResponse>> operations =
      new HashMap<>();
  private ONodeIdentity leader;
  private OrientDBDistributed orientDB;

  public OStructuralSubmitContextImpl(OrientDBDistributed orientDB) {
    this.orientDB = orientDB;
  }

  @Override
  public synchronized Future<OStructuralSubmitResponse> send(
      OSessionOperationId operationId, OStructuralSubmitRequest request) {
    while (leader == null) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    CompletableFuture<OStructuralSubmitResponse> value = new CompletableFuture<>();
    operations.put(operationId, value);
    orientDB.getNetworkManager().submit(leader, operationId, request);
    return value;
  }

  public OStructuralSubmitResponse sendAndWait(
      OSessionOperationId operationId, OStructuralSubmitRequest request) {
    Future<OStructuralSubmitResponse> future = send(operationId, request);
    try {
      return future.get();
    } catch (InterruptedException e) {
      throw OException.wrapException(
          new OInterruptedException("Interrupted waiting for distributed response"), e);
    } catch (ExecutionException e) {
      throw OException.wrapException(
          new ODatabaseException("Error on execution of distributed request"), e);
    }
  }

  @Override
  public synchronized void receive(
      OSessionOperationId requestId, OStructuralSubmitResponse response) {
    CompletableFuture<OStructuralSubmitResponse> future = operations.remove(requestId);
    if (future != null) {
      future.complete(response);
    }
  }

  @Override
  public synchronized void setLeader(ONodeIdentity leader) {
    this.leader = leader;
    notifyAll();
  }
}
