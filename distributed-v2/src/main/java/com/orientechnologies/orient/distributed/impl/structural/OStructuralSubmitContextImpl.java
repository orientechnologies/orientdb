package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedChannel;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class OStructuralSubmitContextImpl implements OStructuralSubmitContext {

  private Map<OSessionOperationId, CompletableFuture<OStructuralSubmitResponse>> operations = new HashMap<>();
  private ODistributedChannel                                                    channel;

  @Override
  public synchronized Future<OStructuralSubmitResponse> send(OSessionOperationId operationId, OStructuralSubmitRequest request) {
    while (channel == null) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    CompletableFuture<OStructuralSubmitResponse> value = new CompletableFuture<>();
    operations.put(operationId, value);
    channel.submit(operationId, request);
    return value;
  }

  public OStructuralSubmitResponse sendAndWait(OSessionOperationId operationId, OStructuralSubmitRequest request) {
    Future<OStructuralSubmitResponse> future = send(operationId, request);
    try {
      return future.get();
    } catch (InterruptedException e) {
      throw OException.wrapException(new OInterruptedException("Interrupted waiting for distributed response"), e);
    } catch (ExecutionException e) {
      throw OException.wrapException(new ODatabaseException("Error on execution of distributed request"), e);
    }
  }

  @Override
  public synchronized void receive(OSessionOperationId requestId, OStructuralSubmitResponse response) {
    CompletableFuture<OStructuralSubmitResponse> future = operations.remove(requestId);
    if (future != null) {
      future.complete(response);
    }
  }

  @Override
  public synchronized void setLeader(ODistributedChannel channel) {
    this.channel = channel;
    notifyAll();
  }

}
