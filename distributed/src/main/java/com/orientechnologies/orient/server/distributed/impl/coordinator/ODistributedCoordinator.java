package com.orientechnologies.orient.server.distributed.impl.coordinator;

import java.util.Map;
import java.util.Timer;
import java.util.concurrent.*;

public class ODistributedCoordinator implements AutoCloseable {

  private ExecutorService                        requestExecutor;
  private OOperationLog                          operationLog;
  private ConcurrentMap<OLogId, ORequestContext> contexts = new ConcurrentHashMap<>();
  private Map<String, ODistributedMember>        members  = new ConcurrentHashMap<>();
  private Timer                                  timer;

  public ODistributedCoordinator(ExecutorService requestExecutor, OOperationLog operationLog) {
    this.requestExecutor = requestExecutor;
    this.operationLog = operationLog;
    this.timer = new Timer(true);
  }

  public void submit(ODistributedMember member, OSubmitRequest request) {
    requestExecutor.execute(() -> {
      request.begin(member, this);
    });
  }

  public void receive(ODistributedMember member, OLogId relativeRequest, ONodeResponse response) {
    requestExecutor.execute(() -> {
      contexts.get(relativeRequest).receive(member, response);
    });
  }

  public OLogId log(ONodeRequest request) {
    return operationLog.log(request);
  }

  public ORequestContext sendOperation(OSubmitRequest submitRequest, ONodeRequest nodeRequest, OResponseHandler handler) {
    OLogId id = log(nodeRequest);
    ORequestContext context = new ORequestContext(this, submitRequest, nodeRequest, members.values(), handler, id);
    contexts.put(id, context);
    for (ODistributedMember member : members.values()) {
      member.sendRequest(id, nodeRequest);
    }
    //Get the timeout from the configuration
    timer.schedule(context.getTimerTask(), 1000, 1000);
    return context;
  }

  public void join(ODistributedMember member) {
    members.put(member.getName(), member);
  }

  @Override
  public void close() {
    timer.cancel();
    requestExecutor.shutdown();
    try {
      requestExecutor.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

  }

  protected void executeOperation(Runnable runnable) {
    requestExecutor.execute(runnable);
  }

  protected void finish(OLogId requestId) {
    contexts.remove(requestId);
  }

  protected ConcurrentMap<OLogId, ORequestContext> getContexts() {
    return contexts;
  }
}
