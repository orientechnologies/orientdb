package com.orientechnologies.orient.server.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBInternal;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ODistributedExecutor implements AutoCloseable {

  private       OOperationLog    operationLog;
  private       ExecutorService  executor;
  private       OrientDBInternal orientDB;
  private final String           database;

  public ODistributedExecutor(ExecutorService executor, OOperationLog operationLog, OrientDB orientDB, String database) {
    this.operationLog = operationLog;
    this.executor = executor;
    this.orientDB = OrientDBInternal.extract(orientDB);
    this.database = database;
  }

  public void receive(ODistributedMember member, OLogId opId, ONodeRequest request) {
    //TODO: sort for opId before execute and execute the operations only if is strictly sequential, otherwise wait.
    executor.execute(() -> {
      operationLog.logReceived(opId, request);
      ONodeResponse response;
      try (ODatabaseDocumentInternal session = orientDB.openNoAuthorization(database)) {
        response = request.execute(member, opId, this, session);
      }
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
    orientDB.close();
  }
}
