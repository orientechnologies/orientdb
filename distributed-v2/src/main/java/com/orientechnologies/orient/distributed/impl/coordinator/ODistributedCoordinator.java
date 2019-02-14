package com.orientechnologies.orient.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ODistributedCoordinator implements AutoCloseable {

  private final ExecutorService                        requestExecutor;
  private final OOperationLog                          operationLog;
  private final ConcurrentMap<OLogId, ORequestContext> contexts = new ConcurrentHashMap<>();
  private final Map<ONodeIdentity, ODistributedMember> members  = new ConcurrentHashMap<>();
  private final Timer                                  timer;
  private final ODistributedLockManager                lockManager;
  private final OClusterPositionAllocator              allocator;

  public ODistributedCoordinator(ExecutorService requestExecutor, OOperationLog operationLog, ODistributedLockManager lockManager,
      OClusterPositionAllocator allocator) {
    this.requestExecutor = requestExecutor;
    this.operationLog = operationLog;
    this.timer = new Timer(true);
    this.lockManager = lockManager;
    this.allocator = allocator;
  }

  public void submit(ODistributedMember member, OSessionOperationId operationId, OSubmitRequest request) {
    requestExecutor.execute(() -> {
      request.begin(member, operationId, this);
    });
  }

  public void reply(ODistributedMember member, OSessionOperationId operationId, OSubmitResponse response) {
    member.reply(operationId, response);
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
    Collection<ODistributedMember> values = new ArrayList<>(members.values());
    ORequestContext context = new ORequestContext(this, submitRequest, nodeRequest, values, handler, id);
    contexts.put(id, context);
    for (ODistributedMember member : values) {
      member.sendRequest(id, nodeRequest);
    }
    //Get the timeout from the configuration
    timer.schedule(context.getTimerTask(), 1000, 1000);
    return context;
  }

  public void join(ODistributedMember member) {
    members.put(member.getNodeIdentity(), member);
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

  protected ConcurrentMap<OLogId, ORequestContext> getContexts() {
    return contexts;
  }

  public ODistributedLockManager getLockManager() {
    return lockManager;
  }

  public OClusterPositionAllocator getAllocator() {
    return allocator;
  }

  public ODistributedMember getMember(ONodeIdentity senderNode) {
    return members.get(senderNode);
  }

  public void leave(ODistributedMember member) {
    members.remove(member.getNodeIdentity());
  }
}
