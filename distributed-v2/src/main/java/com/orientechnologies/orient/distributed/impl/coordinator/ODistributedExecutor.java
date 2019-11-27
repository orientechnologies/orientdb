package com.orientechnologies.orient.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.ODistributedNetwork;
import com.orientechnologies.orient.distributed.impl.ODistributedNetworkManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ODistributedExecutor implements AutoCloseable {

  private final ODistributedNetwork network;
  private       OOperationLog       operationLog;
  private       ExecutorService     executor;
  private       OrientDBInternal    orientDB;
  private final String              database;

  public ODistributedExecutor(ExecutorService executor, OOperationLog operationLog, OrientDBInternal orientDB,
      ODistributedNetwork network, String database) {
    this.operationLog = operationLog;
    this.executor = executor;
    this.orientDB = orientDB;
    this.database = database;
    this.network = network;
  }

  public void receive(ONodeIdentity member, OLogId opId, ONodeRequest request) {
    //TODO: sort for opId before execute and execute the operations only if is strictly sequential, otherwise wait.
    executor.execute(() -> {
      operationLog.logReceived(opId, request);
      ONodeResponse response;
      try (ODatabaseDocumentInternal session = orientDB.openNoAuthorization(database)) {
        response = request.execute(member, opId, this, session);
      }
      network.sendResponse(member, database, opId, response);
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

  public void ping(ONodeIdentity leader, OLogId leaderLastValid) {
    //TODO: Impement
  }
}
