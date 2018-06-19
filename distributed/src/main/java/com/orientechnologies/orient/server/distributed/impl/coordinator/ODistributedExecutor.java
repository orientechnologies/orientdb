package com.orientechnologies.orient.server.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.OrientDB;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ODistributedExecutor implements AutoCloseable {

  private OOperationLog   operationLog;
  private ExecutorService executor;
  private OSender         sender;
  private OrientDB        orientDB;

  public ODistributedExecutor(ExecutorService executor, OOperationLog operationLog, OSender sender, OrientDB orientDB) {
    this.operationLog = operationLog;
    this.executor = executor;
    this.sender = sender;
    this.orientDB = orientDB;
  }

  public void receive(String nodeFrom, OLogId opId, ONodeRequest request) {
    //TODO: sort for opId before execute and execute the operations only if is strictly sequential, otherwise wait.
    executor.execute(() -> {
      operationLog.logReceived(opId, request);
      ONodeResponse response = request.execute(nodeFrom, opId, this);
      send(nodeFrom, opId, response);
    });
  }

  public void send(String node, OLogId opId, ONodeResponse response) {
    sender.sendTo(node, opId, response);
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
