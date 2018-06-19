package com.orientechnologies.orient.server.distributed.impl.coordinator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class ODistributedCoordinator implements AutoCloseable {

  private ExecutorService                        requestExecutor;
  private OOperationLog                          operationLog;
  private ConcurrentMap<OLogId, ORequestContext> contexts = new ConcurrentHashMap<>();
  private List<String>                           nodes    = Collections.synchronizedList(new ArrayList<>());
  private OSender                                sender;

  public ODistributedCoordinator(ExecutorService requestExecutor, OOperationLog operationLog, OSender sender) {
    this.requestExecutor = requestExecutor;
    this.operationLog = operationLog;
    this.sender = sender;
  }

  public void submit(OSubmitRequest request) {
    requestExecutor.execute(() -> {
      request.begin(this);
    });
  }

  public void reply(OSubmitResponse response) {
    sender.sendResponse("one", response);
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
    ORequestContext context = new ORequestContext(this, submitRequest, nodeRequest, nodes.size() / 2 + 1, handler);
    contexts.put(id, context);
    for (String node : nodes) {
      sender.sendTo(node, id, nodeRequest);
    }
    return context;
  }

  public void join(String node) {
    nodes.add(node);
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
