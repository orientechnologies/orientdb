package com.orientechnologies.orient.server.distributed.impl.coordinator;

import java.util.Map;
import java.util.concurrent.*;

public class ODistributedCoordinator implements AutoCloseable {

  private ExecutorService                        requestExecutor;
  private OOperationLog                          operationLog;
  private ConcurrentMap<OLogId, ORequestContext> contexts = new ConcurrentHashMap<>();
  private Map<String, ODistributedMember>        members  = new ConcurrentHashMap<>();

  public ODistributedCoordinator(ExecutorService requestExecutor, OOperationLog operationLog) {
    this.requestExecutor = requestExecutor;
    this.operationLog = operationLog;
  }

  public void submit(ODistributedMember member, OSubmitRequest request) {
    requestExecutor.execute(() -> {
      request.begin(member, this);
    });
  }

  public void receive(OLogId relativeRequest, ONodeResponse response) {
    requestExecutor.execute(() -> {
      contexts.get(relativeRequest).receive(response);
    });
  }

  public OLogId log(ONodeRequest request) {
    return operationLog.log(request);
  }

  public ORequestContext sendOperation(OSubmitRequest submitRequest, ONodeRequest nodeRequest, OResponseHandler handler) {
    OLogId id = log(nodeRequest);
    ORequestContext context = new ORequestContext(this, submitRequest, nodeRequest, members.values(), handler);
    contexts.put(id, context);
    for (ODistributedMember member : members.values()) {
      member.sendRequest(id, nodeRequest);
    }
    return context;
  }

  public void join(ODistributedMember member) {
    members.put(member.getName(), member);
  }

  @Override
  public void close() {
    requestExecutor.shutdown();
    try {
      requestExecutor.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

  }
}
