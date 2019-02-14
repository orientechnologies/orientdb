package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.*;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class OStructuralCoordinator implements AutoCloseable {
  private final ExecutorService                                  requestExecutor;
  private final OOperationLog                                    operationLog;
  private final ConcurrentMap<OLogId, OStructuralRequestContext> contexts = new ConcurrentHashMap<>();
  private final Map<ONodeIdentity, OStructuralDistributedMember> members  = new ConcurrentHashMap<>();
  private final Timer                                            timer;
  private final OrientDBDistributed                              context;

  public OStructuralCoordinator(ExecutorService requestExecutor, OOperationLog operationLog, OrientDBDistributed context) {
    this.requestExecutor = requestExecutor;
    this.operationLog = operationLog;
    this.timer = new Timer(true);
    this.context = context;
  }

  public void submit(OStructuralDistributedMember member, OSessionOperationId operationId, OStructuralSubmitRequest request) {
    requestExecutor.execute(() -> {
      request.begin(member, operationId, this, context);
    });
  }

  public void reply(OStructuralDistributedMember member, OSessionOperationId operationId, OStructuralSubmitResponse response) {
    member.reply(operationId, response);
  }

  public void receive(OStructuralDistributedMember member, OLogId relativeRequest, OStructuralNodeResponse response) {
    requestExecutor.execute(() -> {
      contexts.get(relativeRequest).receive(member, response);
    });
  }

  public OLogId log(OStructuralNodeRequest request) {
    return operationLog.log(request);
  }

  public OStructuralRequestContext sendOperation(OStructuralSubmitRequest submitRequest, OStructuralNodeRequest nodeRequest,
      OStructuralResponseHandler handler) {
    List<OStructuralDistributedMember> members = new ArrayList<>(this.members.values());
    OLogId id = log(nodeRequest);
    OStructuralRequestContext context = new OStructuralRequestContext(this, submitRequest, nodeRequest, members, handler, id);
    contexts.put(id, context);
    for (OStructuralDistributedMember member : members) {
      member.sendRequest(id, nodeRequest);
    }
    //Get the timeout from the configuration
    timer.schedule(context.getTimerTask(), 1000, 1000);
    return context;
  }

  public void nodeConnected(OStructuralDistributedMember member) {
    members.put(member.getIdentity(), member);
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

  public void executeOperation(Runnable runnable) {
    requestExecutor.execute(runnable);
  }

  public void finish(OLogId requestId) {
    contexts.remove(requestId);
  }

  public ConcurrentMap<OLogId, OStructuralRequestContext> getContexts() {
    return contexts;
  }

  public OStructuralDistributedMember getMember(ONodeIdentity member) {
    return members.get(member);
  }

  public void nodeDisconnected(ONodeIdentity identity) {
    // TODO: Instead of remove mark it as disconnected
    members.remove(identity);
  }

}
