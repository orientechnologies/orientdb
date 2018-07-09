package com.orientechnologies.orient.server.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.OrientDB;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ODistributedExecutor implements AutoCloseable {

  private OOperationLog   operationLog;
  private ExecutorService executor;
  private OrientDB        orientDB;

  public ODistributedExecutor(ExecutorService executor, OOperationLog operationLog, OrientDB orientDB) {
    this.operationLog = operationLog;
    this.executor = executor;
    this.orientDB = orientDB;
  }

  public void receive(ODistributedMember member, OLogId opId, ONodeRequest request) {
    //TODO: sort for opId before execute and execute the operations only if is strictly sequential, otherwise wait.
    executor.execute(() -> {
      operationLog.logReceived(opId, request);
      ONodeResponse response = request.execute(member, opId, this);
      member.sendResponse(opId, response);
    });
  }

  public OrientDB getOrientDB() {
    return orientDB;
  }

  @Override
  public void close() {
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    orientDB.close();
  }
}
