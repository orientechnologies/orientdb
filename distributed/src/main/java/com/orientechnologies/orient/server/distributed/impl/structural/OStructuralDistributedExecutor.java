package com.orientechnologies.orient.server.distributed.impl.structural;

import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class OStructuralDistributedExecutor implements AutoCloseable {

  private       OOperationLog                             operationLog;
  private       ExecutorService                           executor;
  private       OrientDBInternal                          orientDB;
  private final Map<String, OStructuralDistributedMember> members = new ConcurrentHashMap<>();

  public OStructuralDistributedExecutor(ExecutorService executor, OOperationLog operationLog, OrientDBInternal orientDB) {
    this.operationLog = operationLog;
    this.executor = executor;
    this.orientDB = orientDB;
  }

  public void receive(OStructuralDistributedMember member, OLogId opId, OStructuralNodeRequest request) {
    //TODO: sort for opId before execute and execute the operations only if is strictly sequential, otherwise wait.
    executor.execute(() -> {
      operationLog.logReceived(opId, request);
      OStructuralNodeResponse response;
      response = request.execute(member, opId, this, orientDB);
      member.sendResponse(opId, response);
    });
  }

  @Override
  public void close() {
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void join(OStructuralDistributedMember member) {
    members.put(member.getName(), member);
  }

  public OStructuralDistributedMember getMember(String senderNode) {
    return members.get(senderNode);
  }

  public void leave(OStructuralDistributedMember member) {
    members.remove(member.getName());
  }
}
